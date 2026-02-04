#!/usr/bin/env python3
"""
Post-processing script for spike sorting results.
Takes the output of run_shank.py and performs UnitRefine classification.
"""

import sys
import os
import json
import numpy as np
from pathlib import Path
import spikeinterface as si
import spikeinterface.extractors as se
import spikeinterface.curation as sc
import probeinterface as pi

# Constants
SAMPLE_RATE = 30000
N_CHANNELS_PROBE = 384

def find_active_channels(probe_dict):
    """Find active channels in probe configuration."""
    dev_ind = np.array(probe_dict['probes'][0]["device_channel_indices"])
    return dev_ind != -1

def find_shank_channels(probe_dict, shank_num):
    """Find channels belonging to a specific shank."""
    shank_ind = np.array(probe_dict["shank_ids"])
    shank_num_str = str(shank_num)
    return np.array([str(s) == shank_num_str for s in shank_ind])

def load_probe(probe_file, shank_num=None, n_channels_shank=None):
    """
    Load probe configuration from JSON file.
    
    Args:
        probe_file: Path to probe configuration JSON file
        shank_num: Optional shank number to filter channels
        n_channels_shank: Optional number of channels per shank
        
    Returns:
        probe: ProbeInterface probe object
        n_channels: Number of channels (None if shank_num not specified)
    """
    with open(probe_file, 'r') as f:
        probe_dict = json.load(f)
    
    active_channels_mask = find_active_channels(probe_dict)
    probe = probe_dict['probes'][0]  # get the first probe

    # First filter by active channels
    for key in ['contact_positions', 'contact_plane_axes', 'contact_shapes', 
                'contact_shape_params', 'device_channel_indices', 'contact_ids', 'shank_ids']:
        probe[key] = np.array(probe[key])[active_channels_mask]

    # Then filter by shank if specified
    n_channels = None
    if shank_num is not None:
        shank_channels_mask = find_shank_channels(probe, shank_num)
        for key in ['contact_positions', 'contact_plane_axes', 'contact_shapes', 
                    'contact_shape_params', 'device_channel_indices', 'contact_ids', 'shank_ids']:
            probe[key] = probe[key][shank_channels_mask]
        # Count actual channels from JSON after filtering
        n_channels = len(probe['device_channel_indices'])
        # Use provided n_channels_shank if given, otherwise use count from JSON
        channels_count = n_channels_shank if n_channels_shank is not None else n_channels
        probe['device_channel_indices'] = np.arange(0, channels_count)

    probe = pi.Probe.from_dict(probe)
    return probe, n_channels

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

def save_results(sorting_analyzer, labels, shank_folder):
    """
    Save sorting analyzer and labels to disk.
    
    Args:
        sorting_analyzer: SortingAnalyzer object
        labels: DataFrame with unit classifications
        shank_folder: Path to shank output folder
    """
    # Save sorting analyzer
    analyzer_path = shank_folder / 'kilosort4' / 'sorting_analyzer'
    print(f"\nSaving SortingAnalyzer to {analyzer_path}")
    sorting_analyzer.save_as(folder=str(analyzer_path), format="zarr")
    
    # Save labels
    labels_path = shank_folder / 'unit_labels.csv'
    print(f"Saving unit labels to {labels_path}")
    labels.to_csv(labels_path, index=True)
    
    print("Post-processing complete!")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: postproc.py <folder> <probe> <shank_num> [model_repo_id]")
        print("  folder: Base folder containing data and output directories")
        print("  probe: Probe name (e.g., 'a', 'b')")
        print("  shank_num: Shank number (e.g., 1, 2, 3, 4)")
        print("  model_repo_id: Optional model repository ID (default: SUA/MUA classifier)")
        sys.exit(1)
    
    folder = Path(sys.argv[1])
    probe = sys.argv[2]
    shank_num = sys.argv[3]
    model_repo_id = sys.argv[4] if len(sys.argv) > 4 else None
    
    # Set up paths
    output_folder = folder / "output"
    probe_folder = output_folder / probe
    shank_folder = probe_folder / f"shank_{shank_num}"
    probe_file = folder / f"{probe}_probe_conf.json"
    
    print(f"Post-processing shank {shank_num} for probe {probe}")
    print(f"Folder: {folder}")
    print(f"Shank folder: {shank_folder}")
    
    # Load probe configuration
    print(f"\nLoading probe configuration from {probe_file}")
    shank_probe, n_channels_shank = load_probe(probe_file, shank_num)
    
    if n_channels_shank is None or n_channels_shank == 0:
        print(f"ERROR: Shank {shank_num} has zero channels. Exiting.")
        sys.exit(1)
    
    print(f"Number of channels in shank: {n_channels_shank}")
    
    # Load recording and sorting
    recording, sorting = load_recording_and_sorting(shank_folder, shank_probe, n_channels_shank)
    
    print(f"Recording: {recording.get_num_channels()} channels, "
          f"{recording.get_total_duration():.2f} seconds")
    print(f"Sorting: {len(sorting.unit_ids)} units")
    
    # Create SortingAnalyzer and compute extensions
    sorting_analyzer = compute_sorting_analyzer(recording, sorting, n_jobs=12)
    
    # Apply UnitRefine classification
    labels, model_info = apply_unitrefine_classification(sorting_analyzer, model_repo_id)
    
    # Save results
    save_results(sorting_analyzer, labels, shank_folder)
    
    print(f"\nPost-processing complete for {probe} shank {shank_num}!")


#python postproc.py /groups/voigts/voigtslab/neuropixels_2025/npx10/2025_12_18_large_maze/ a 0
