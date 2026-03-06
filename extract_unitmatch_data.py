#!/usr/bin/env python3
"""
Neuropixels Unit Data Extraction Script

This script extracts and processes unit data from KiloSort4 output, including:
- Average waveforms over time
- Position calculations
- Channel statistics
- Spike counts

Based on the original get_units.ipynb notebook, but rewritten for reusability.

Usage:
    python extract_unit_data.py --data_dir /path/to/data --probe a --shank 0
    python extract_unit_data.py --data_dir '/groups/voigts/voigtslab/neuropixels_2025/npx10/2025_12_18_large_maze' --probe a --shank 0 --n_jobs 16 --verbose 10
    python extract_unit_data.py --all_probes --data_dir /path/to/data
"""

import argparse
import json
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Union
from joblib import Parallel, delayed
from scipy.ndimage import gaussian_filter
import warnings
warnings.filterwarnings('ignore')

class UnitDataExtractor:
    """Extract and process unit data from KiloSort4 output.

    Loads raw recording data and KiloSort4 results, then computes summary
    waveforms, bootstrap-based position estimates, and related statistics per
    unit in fixed time windows.
    """
    
    def __init__(self, data_dir: Union[str, Path], probe_letter: str = 'a', shank_num: int = 0):
        """
        Initialize the extractor for a specific probe and shank
        
        Args:
            data_dir: Base directory containing the neuropixels data
            probe_letter: Probe letter ('a' or 'b')
            shank_num: Shank number (0, 1, 2, or 3)
        """
        self.data_dir = Path(data_dir)
        self.probe_letter = probe_letter
        self.shank_num = shank_num
        
        # Set up paths
        self.common_dir = self.data_dir / 'output' / probe_letter / f'shank_{shank_num}'
        self.ks_dir = self.common_dir / 'kilosort4'
        self.data_path = self.common_dir / 'shank_recording.bin'
        self.output_dir = self.ks_dir / 'RawWaveforms'
        
        # Recording parameters
        self.n_channels = 96
        
        # Waveform extraction parameters
        self.samples_before = 20
        self.spike_width = 61
        self.samples_after = self.spike_width - self.samples_before
        self.half_width = int(np.floor(self.spike_width / 2))
        #self.time_window = 30000 * 60 * 5  # 5 minutes in samples
        
        # Quality control parameters
        self.max_samples = 5000
        self.min_samples = 100
        
        # Initialize data arrays
        self.data = None
        self.clu = None
        self.spike_times = None
        self.amp = None
        self.pos = None
        self.cluster_names = None

    def load_recording_data(self) -> None:
        """Load raw recording data and KiloSort4 output arrays for this shank."""
        print(f"Loading recording data for probe {self.probe_letter}, shank {self.shank_num}...")
        
        # Load raw data
        if not self.data_path.exists():
            raise FileNotFoundError(f"Recording data not found: {self.data_path}")
        
        n_samples = self.data_path.stat().st_size // (self.n_channels * np.dtype('int16').itemsize)
        self.data = np.memmap(self.data_path, dtype='int16', shape=(n_samples, self.n_channels))
        
        # Load KiloSort4 output
        self.clu = np.load(self.ks_dir / 'spike_clusters.npy')
        self.spike_times = np.load(self.ks_dir / 'spike_times.npy')
        self.pos = np.load(self.ks_dir / 'spike_positions.npy')
        self.amp = np.load(self.ks_dir / 'amplitudes.npy')
        
        # Basic consistency check
        if not (self.clu.shape[0] == self.spike_times.shape[0] == self.amp.shape[0]):
            raise ValueError(
                f"Mismatched array lengths: clusters={self.clu.shape[0]}, times={self.spike_times.shape[0]}, amplitudes={self.amp.shape[0]}"
            )
        
        self.cluster_names = list(set(self.clu))
        
        print(f"Loaded {len(self.cluster_names)} clusters")
        print(f"Data shape: {self.data.shape}")
        print(f"Spike times shape: {self.spike_times.shape}")
    
    
    def extract_unit_waveform(self, sample_idx: np.ndarray, sample_amount: int) -> np.ndarray:
        """Extract per-spike waveforms for a unit.

        Args:
            sample_idx: Spike indices (sample positions) for this unit.
            sample_amount: Number of spikes to extract (upper bound).

        Returns:
            A float32 array of shape (sample_amount, spike_width, n_channels).
        """
        channels = np.arange(0, self.n_channels)
        all_sample_waveforms = np.zeros((sample_amount, self.spike_width, self.n_channels))
        
        for i, idx in enumerate(sample_idx[:sample_amount]):
            if np.isnan(idx):
                continue
            
            # Extract waveform around spike
            start_idx = int(idx - self.samples_before - 1)
            end_idx = int(idx + self.samples_after - 1)
            tmp = self.data[start_idx:end_idx, channels].astype(np.float32)
            
            # Apply Gaussian smoothing
            tmp = gaussian_filter(tmp, 1, radius=2, axes=0)
            
            # Baseline correction
            tmp = tmp - np.mean(tmp[:20, :], axis=0)
            all_sample_waveforms[i] = tmp
        
        return all_sample_waveforms


    
    def process_unit(self, unit_id: int) -> Dict[str, np.ndarray]:
        """Process a single unit and extract two waveforms (one for each half of the recording).

        Args:
            unit_id: Cluster ID of the unit to process.

        Returns:
            Dictionary containing two waveforms (one for each half of the recording).
        """
        # Filter spikes for this unit
        spike_filter = np.logical_and(
            (self.spike_times > self.half_width),
            (self.spike_times < (self.data.shape[0] - self.half_width - 9))
        )
        
        clu_filtered = self.clu[spike_filter]
        spike_times_filtered = self.spike_times[spike_filter]
        
        # Split recording exactly in half
        min_time = np.min(spike_times_filtered)
        max_time = np.max(spike_times_filtered)
        midpoint = (min_time + max_time) / 2

        # Initialize output array for two waveforms
        avg_waveform = np.zeros((self.spike_width, self.n_channels, 2))

        # Process first half (before midpoint)
        first_half_filter = spike_times_filtered <= midpoint
        clu_first = clu_filtered[first_half_filter]
        spike_times_first = spike_times_filtered[first_half_filter]
        
        unit_mask_first = (clu_first == unit_id)
        st_unit_first = spike_times_first[unit_mask_first]
        sample_amount_first = len(st_unit_first)
        
        if sample_amount_first > self.max_samples:
            samples = np.random.choice(sample_amount_first, self.max_samples, replace=False)
            st_unit_first = st_unit_first[samples]
            sample_amount_first = self.max_samples
        
        if sample_amount_first > self.min_samples:
            all_samples_first = self.extract_unit_waveform(st_unit_first, sample_amount_first)
            avg_waveform[:,:,0] = np.median(all_samples_first, axis=0)
        else:
            avg_waveform[:,:,0] = np.zeros((self.spike_width, self.n_channels))

        # Process second half (after midpoint)
        second_half_filter = spike_times_filtered > midpoint
        clu_second = clu_filtered[second_half_filter]
        spike_times_second = spike_times_filtered[second_half_filter]
        
        unit_mask_second = (clu_second == unit_id)
        st_unit_second = spike_times_second[unit_mask_second]
        sample_amount_second = len(st_unit_second)
        
        if sample_amount_second > self.max_samples:
            samples = np.random.choice(sample_amount_second, self.max_samples, replace=False)
            st_unit_second = st_unit_second[samples]
            sample_amount_second = self.max_samples
        
        if sample_amount_second > self.min_samples:
            all_samples_second = self.extract_unit_waveform(st_unit_second, sample_amount_second)
            avg_waveform[:,:,1] = np.median(all_samples_second, axis=0)
        else:
            avg_waveform[:,:,1] = np.zeros((self.spike_width, self.n_channels))

        return {
            'RawSpikes': avg_waveform
        }
    
    def save_unit_data(self, unit_id: int, unit_data: Dict[str, np.ndarray]) -> None:
        """Save unit data to files"""
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save each data type
        for data_type, data_array in unit_data.items():
            file_path = self.output_dir / f'Unit{unit_id}_{data_type}.npy'
            np.save(file_path, data_array)
    
    def process_all_units(self, n_jobs: int = -1, verbose: int = 10) -> None:
        """
        Process all units for this probe and shank
        
        Args:
            n_jobs: Number of parallel jobs (-1 for all cores)
            verbose: Verbosity level for parallel processing
        """
        if self.data is None:
            self.load_recording_data()
        
        print(f"Processing {len(self.cluster_names)} units...")
        
        # Process units in parallel
        results = Parallel(n_jobs=n_jobs, verbose=verbose)(
            delayed(self._process_and_save_unit)(unit_id) 
            for unit_id in self.cluster_names
        )
        
        print(f"Completed processing {len(self.cluster_names)} units")
    
    def _process_and_save_unit(self, unit_id: int) -> None:
        """Process and save a single unit (for parallel processing)"""
        try:
            unit_data = self.process_unit(unit_id)
            self.save_unit_data(unit_id, unit_data)
        except Exception as e:
            print(f"Error processing unit {unit_id}: {e}")

def process_all_probes_and_shanks(data_dir: Union[str, Path], 
                                 probes: List[str] = ['a', 'b'], 
                                 shanks: List[int] = [0, 1, 2, 3],
                                 n_jobs: int = -1) -> None:
    """
    Process all probes and shanks
    
    Args:
        data_dir: Base directory containing the neuropixels data
        probes: List of probe letters to process
        shanks: List of shank numbers to process
        n_jobs: Number of parallel jobs for each probe/shank
    """
    data_dir = Path(data_dir)
    
    for probe in probes:
        for shank in shanks:
            print(f"\n{'='*60}")
            print(f"Processing probe {probe}, shank {shank}")
            print(f"{'='*60}")
            
            try:
                extractor = UnitDataExtractor(data_dir, probe, shank)
                extractor.process_all_units(n_jobs=n_jobs)
            except Exception as e:
                print(f"Error processing probe {probe}, shank {shank}: {e}")
                continue

def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(description='Extract unit data from KiloSort4 output')
    parser.add_argument('--data_dir', type=str, required=True,
                       help='Base directory containing neuropixels data')
    parser.add_argument('--probe', type=str, choices=['a', 'b'],
                       help='Probe letter to process (if not processing all)')
    parser.add_argument('--shank', type=int, choices=[0, 1, 2, 3],
                       help='Shank number to process (if not processing all)')
    parser.add_argument('--all_probes', action='store_true',
                       help='Process all probes and shanks')
    parser.add_argument('--n_jobs', type=int, default=-1,
                       help='Number of parallel jobs (-1 for all cores)')
    parser.add_argument('--verbose', type=int, default=10,
                       help='Verbosity level for parallel processing')
    
    args = parser.parse_args()
    
    if args.all_probes:
        process_all_probes_and_shanks(args.data_dir, n_jobs=args.n_jobs)
    elif args.probe is not None and args.shank is not None:
        extractor = UnitDataExtractor(args.data_dir, args.probe, args.shank)
        extractor.process_all_units(n_jobs=args.n_jobs, verbose=args.verbose)
    else:
        print("Error: Must specify either --all_probes or both --probe and --shank")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())