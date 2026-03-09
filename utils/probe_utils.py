#!/usr/bin/env python3
"""
Probe loading utilities for neuropixels data processing.

This module provides common functions for loading and processing probe configurations
from JSON files, including filtering by active channels and shank numbers.
"""

import json
import numpy as np
import probeinterface as pi


def find_active_channels(probe_dict: dict) -> np.ndarray:
    """Find active channels in probe configuration."""
    dev_ind = np.array(probe_dict['probes'][0]["device_channel_indices"])
    return dev_ind != -1


def find_shank_channels(probe_dict: dict, shank_num: int | str) -> np.ndarray:
    """Find channels belonging to a specific shank."""
    shank_ind = np.array(probe_dict["shank_ids"])
    shank_num_str = str(shank_num)
    return np.array([str(s) == shank_num_str for s in shank_ind])


def load_probe(
    probe_file: str | Path,
    shank_num: int | str | None = None,
    n_channels_shank: int | None = None,
) -> tuple[pi.Probe, int | None]:
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
