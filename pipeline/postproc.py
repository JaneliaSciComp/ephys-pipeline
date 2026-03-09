#!/usr/bin/env python3
"""
Post-processing script for spike sorting results.
Takes the output of run_shank.py and performs UnitRefine classification.
"""

import sys
import os
import json
import numpy as np
import pandas as pd
from pathlib import Path
import spikeinterface as si
import spikeinterface.extractors as se
import spikeinterface.curation as sc
import probeinterface as pi
from utils.probe_utils import load_probe

# Constants
SAMPLE_RATE = 30000
N_CHANNELS_PROBE = 384

def load_recording_and_sorting(shank_folder, probe, n_channels_shank):
    """
    Load recording and sorting from run_shank.py output.
    
    Args:
        shank_folder: Path to shank output folder
        probe: ProbeInterface probe object
        n_channels_shank: Number of channels in the shank
        
    Returns:
        recording: SpikeInterface recording object
        sorting: SpikeInterface sorting object
    """
    recording_path = shank_folder / 'shank_recording.bin'
    sorting_path = shank_folder / 'kilosort4'
    
    if not recording_path.exists():
        raise FileNotFoundError(f"Recording file not found: {recording_path}")
    if not sorting_path.exists():
        raise FileNotFoundError(f"Sorting folder not found: {sorting_path}")
    
    print(f"Loading recording from {recording_path}")
    recording = se.read_binary(
        file_paths=str(recording_path),
        sampling_frequency=SAMPLE_RATE,
        num_channels=n_channels_shank,
        dtype="int16",
        offset_to_uV=0.0,
        gain_to_uV=1.0
    )
    recording = recording.set_probe(probe)
    
    print(f"Loading sorting from {sorting_path}")
    sorting = se.read_phy(folder_path=str(sorting_path))
    
    return recording, sorting

def load_sorting_analyzer(analyzer_path):
    """
    Load an existing SortingAnalyzer from disk.
    
    Args:
        analyzer_path: Path to the sorting analyzer folder (without .zarr extension)
        
    Returns:
        sorting_analyzer: SortingAnalyzer object
    """
    # SpikeInterface saves as .zarr directory, so add the extension
    analyzer_path_zarr = analyzer_path.with_suffix('.zarr') if not str(analyzer_path).endswith('.zarr') else analyzer_path
    
    print(f"\nLoading existing SortingAnalyzer from {analyzer_path_zarr}")
    sorting_analyzer = si.load_sorting_analyzer(folder=str(analyzer_path_zarr))
    
    # Display computed metrics
    quality_metrics = list(sorting_analyzer.get_extension('quality_metrics').get_data().keys())
    template_metrics = list(sorting_analyzer.get_extension('template_metrics').get_data().keys())
    all_computed_metrics = quality_metrics + template_metrics
    print(f"Loaded analyzer with {len(all_computed_metrics)} computed metrics.")
    
    return sorting_analyzer

def compute_sorting_analyzer(recording, sorting, n_jobs=12):
    """
    Create SortingAnalyzer and compute all required extensions.
    
    Args:
        recording: SpikeInterface recording object
        sorting: SpikeInterface sorting object
        n_jobs: Number of parallel jobs
        
    Returns:
        sorting_analyzer: SortingAnalyzer object with computed extensions
    """
    print("\nCreating SortingAnalyzer and computing extensions...")
    sorting_analyzer = si.create_sorting_analyzer(
        sorting=sorting,
        recording=recording,
        sparse=False
    )
    
    # Compute all extensions required by the model
    extensions_to_compute = [
        'noise_levels', 'random_spikes',
        'waveforms', 'templates',
        'spike_locations', 'spike_amplitudes',
        'correlograms',
        'quality_metrics', 'template_metrics'
    ]
    
    print(f"Computing extensions: {', '.join(extensions_to_compute)}")
    sorting_analyzer.compute(extensions_to_compute, n_jobs=n_jobs)
    
    # Compute template metrics with multi-channel metrics
    print("Computing template metrics with multi-channel metrics...")
    sorting_analyzer.compute("template_metrics", include_multi_channel_metrics=True, n_jobs=n_jobs)
    
    # Display computed metrics
    quality_metrics = list(sorting_analyzer.get_extension('quality_metrics').get_data().keys())
    template_metrics = list(sorting_analyzer.get_extension('template_metrics').get_data().keys())
    all_computed_metrics = quality_metrics + template_metrics
    print(f"\n{len(all_computed_metrics)} metrics computed.")
    
    return sorting_analyzer

def save_unitrefine_dataset(sorting_analyzer, shank_folder):
    """
    Save the feature dataset that is provided to the UnitRefine model.
    
    This is constructed from the quality and template metrics extensions of the
    SortingAnalyzer, which are the inputs used by the UnitRefine classifiers.
    """
    # Collect feature tables used by the model
    qm_ext = sorting_analyzer.get_extension('quality_metrics')
    tm_ext = sorting_analyzer.get_extension('template_metrics')

    qm_df = qm_ext.get_data()
    tm_df = tm_ext.get_data()

    # Combine into a single feature matrix
    features_df = pd.concat([qm_df, tm_df], axis=1)

    # Save alongside the other Kilosort outputs
    dataset_path = shank_folder / 'kilosort4' / 'unitrefine_input_metrics.tsv'
    print(f"\nSaving UnitRefine input dataset to {dataset_path}")
    features_df.to_csv(dataset_path, sep='\t', index=True)

def apply_unitrefine_classification(sorting_analyzer, model_repo_id=None):
    """
    Apply UnitRefine model to classify units.
    
    Args:
        sorting_analyzer: SortingAnalyzer object with computed extensions
        model_repo_id: Optional model repository ID. Defaults to SUA/MUA classifier.
        
    Returns:
        labels: DataFrame with unit classifications
        model_info: Model information dictionary
    """
    if model_repo_id is None:
        # Use the SUA/MUA classifier by default (as in the copy notebook)
        model_repo_id = "SpikeInterface/UnitRefine_sua_mua_classifier_lightweight"
    
    print(f"\nLoading model: {model_repo_id}")
    model, model_info = sc.load_model(
        repo_id=model_repo_id,
        trusted=['numpy.dtype']
    )
    
    print("Applying pre-trained model to classify units...")
    labels = sc.auto_label_units(
        sorting_analyzer=sorting_analyzer,
        repo_id=model_repo_id,
        trusted=['numpy.dtype']
    )
    
    # Display prediction summary
    label_counts = labels.iloc[:, 0].value_counts()
    print(f"\nPrediction Summary:")
    for label, count in label_counts.items():
        percentage = (count / len(labels)) * 100
        print(f"  {label}: {count} units ({percentage:.1f}%)")


    return labels, model_info

def save_results(sorting_analyzer, all_labels_df, shank_folder, models_to_run=None):
    """
    Save sorting analyzer and labels to disk.
    
    Args:
        sorting_analyzer: SortingAnalyzer object
        all_labels_df: DataFrame with concatenated labels from all models
        shank_folder: Path to shank output folder
        analyzer_already_exists: If True, skip saving the analyzer (only save labels)
        models_to_run: List of model names for column renaming
    """
    analyzer_path = shank_folder / 'kilosort4' / 'sorting_analyzer'
    analyzer_path_zarr = analyzer_path.with_suffix('.zarr')
    
    print(f"\nSaving SortingAnalyzer to {analyzer_path_zarr}")
    sorting_analyzer.save_as(folder=str(analyzer_path), format="zarr")
 
    # Rename columns to indicate which model they come from
    if models_to_run is not None and len(all_labels_df.columns) == len(models_to_run) * 2:
        # Rename columns based on model type
        new_columns = []
        for i, model_name in enumerate(models_to_run):
            model_part = model_name.split('/')[-1]
            # Extract model type: "sua_mua" or "noise_neural"
            if 'sua_mua' in model_part:
                model_type = 'sua'
            elif 'noise_neural' in model_part:
                model_type = 'noise'
            else:
                model_type = model_part.replace('UnitRefine_', '').replace('_classifier_lightweight', '')
            
            # Each model has 2 columns (prediction and confidence)
            col_idx = i * 2
            new_columns.append(f'{model_type}_prediction')
            new_columns.append(f'{model_type}_confidence')
        
        all_labels_df.columns = new_columns
    else:
        print(f"Warning: Column count mismatch. Expected {len(models_to_run) * 2 if models_to_run else 'unknown'}, got {len(all_labels_df.columns)}")
    
    # Save combined labels to single CSV
    labels_path = shank_folder / 'kilosort4' / 'unit_labels.tsv'
    print(f"\nSaving combined unit labels to {labels_path}")
    all_labels_df.to_csv(labels_path, index=True)
    
    print("Post-processing complete!")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: postproc.py <folder> <probe> <shank_num>")
        print("  folder: Base folder containing data and output directories")
        print("  probe: Probe name (e.g., 'a', 'b')")
        print("  shank_num: Shank number (e.g., 1, 2, 3, 4)")
        print("\nThis script runs two models:")
        print("  - SpikeInterface/UnitRefine_sua_mua_classifier_lightweight")
        print("  - SpikeInterface/UnitRefine_noise_neural_classifier_lightweight")
        sys.exit(1)
    
    folder = Path(sys.argv[1])
    probe = sys.argv[2]
    shank_num = sys.argv[3]
    
    # Define models to run
    models_to_run = [
        "SpikeInterface/UnitRefine_sua_mua_classifier_lightweight",
        "SpikeInterface/UnitRefine_noise_neural_classifier_lightweight"
    ]
    
    # Set up paths
    output_folder = folder / "output"
    probe_folder = output_folder / probe
    shank_folder = probe_folder / f"shank_{shank_num}"
    probe_file = folder / f"{probe}_probe_conf.json"
    
    print(f"Post-processing shank {shank_num} for probe {probe}")
    print(f"Folder: {folder}")
    print(f"Shank folder: {shank_folder}")
    print(f"\nRunning {len(models_to_run)} models:")
    for model in models_to_run:
        print(f"  - {model}")
    
    # Load probe configuration
    print(f"\nLoading probe configuration from {probe_file}")
    shank_probe, n_channels_shank = load_probe(probe_file, shank_num)
    
    if n_channels_shank is None or n_channels_shank == 0:
        print(f"ERROR: Shank {shank_num} has zero channels. Exiting.")
        sys.exit(1)
    
    print(f"Number of channels in shank: {n_channels_shank}")
    
    # Check if sorting analyzer already exists
    # SpikeInterface saves as .zarr directory, so check for that
    analyzer_path = shank_folder / 'kilosort4' / 'sorting_analyzer'
    
    # Load recording and sorting
    recording, sorting = load_recording_and_sorting(shank_folder, shank_probe, n_channels_shank)
    
    print(f"Recording: {recording.get_num_channels()} channels, "
            f"{recording.get_total_duration():.2f} seconds")
    print(f"Sorting: {len(sorting.unit_ids)} units")
    
    # Create SortingAnalyzer and compute extensions
    sorting_analyzer = compute_sorting_analyzer(recording, sorting, n_jobs=12)
    
    # Save the dataset that will be provided to the UnitRefine models
    save_unitrefine_dataset(sorting_analyzer, shank_folder)

    # Apply UnitRefine classification for each model
    all_labels = []
    for i, model_repo_id in enumerate(models_to_run, 1):
        print(f"\n{'='*60}")
        print(f"Running model {i}/{len(models_to_run)}: {model_repo_id}")
        print(f"{'='*60}")
        labels, model_info = apply_unitrefine_classification(sorting_analyzer, model_repo_id)
        all_labels.append(labels)

    all_labels = pd.concat(all_labels, axis=1)
    
    # Save results
    save_results(sorting_analyzer, all_labels, shank_folder, models_to_run=models_to_run)
    
    print(f"\nPost-processing complete for {probe} shank {shank_num}!")
