#!/usr/bin/env python3
"""
Probe loading utilities for neuropixels data processing.

This module provides common functions for loading and processing probe configurations
from JSON files, including filtering by active channels and shank numbers.

Note that the probe json gets loaded, but then its fields are reordered to all
match the order of the channels in the recording .bin, so by channel, not physical contact layout.

"""

from __future__ import annotations

import json
import numpy as np
import probeinterface as pi
from pathlib import Path
import copy

PROBE_ARRAY_KEYS = [
    'contact_positions',
    'contact_plane_axes',
    'contact_shapes',
    'contact_shape_params',
    'device_channel_indices',
    'contact_ids',
    'shank_ids'
]

def find_active_channels(probe_dict: dict) -> np.ndarray:
    """Find active channels in probe configuration."""
    dev_ind = np.array(probe_dict['probes'][0]["device_channel_indices"])
    return dev_ind != -1


def find_shank_channels(probe_dict: dict, shank_num: int | str) -> np.ndarray:
    """Find channels belonging to a specific shank."""
    shank_ind = np.array(probe_dict["shank_ids"])
    shank_num_str = str(shank_num)
    return np.array([str(s) == shank_num_str for s in shank_ind])

def sort_probe_arrays_by_device_chan(probe: dict) -> None:
    """
    Sort active probe rows so row i corresponds to channel i.
    This is because the probe json info is in physical contact and shank order.
    However, the raw .bin is in channel order, so use the device_channel_indices
    to map the json info to match up to the raw data.

    This assumes also that inactive contacts have been removed already.
    """
    # check some basics
    dev_ind = np.asarray(probe["device_channel_indices"], dtype=int)
    if np.any(dev_ind == -1):
        raise ValueError(
            "inactive contacts remain in probe json arrays, filter out device_channel_indices == -1 before this."
        )
    
    if len(np.unique(dev_ind)) != len(dev_ind):
        raise ValueError(
            "Duplicates found in device_channel_indices, so can't sort probe arrays by device channel index."
        )
    
    expected = np.arange(len(dev_ind))
    if not np.array_equal(np.sort(dev_ind), expected):
        raise ValueError(
            f"device_channel_indices should contain all ints from 0 to {len(dev_ind)-1} with no duplicates, but got {np.sort(dev_ind)}"
        )       

    order = np.argsort(dev_ind) # sort active device chan indices ascending, to match raw .bin data

    for key in PROBE_ARRAY_KEYS:
        probe[key] = np.asarray(probe[key])[order] # reorder json fields
        # or consider making new probe obj that has the reordered metadata rather than in place modification
        # then have fxn return that new probe dict and convert to pi obj after?

def load_probe(
    probe_file: str | Path,
    shank_num: int | str | None = None,
    n_channels_shank: int | None = None,
) -> tuple[pi.Probe, int | None]:
    """
    Load probe configuration/ProbeInterface probe object from JSON file.
    
    Note that the probe json gets loaded, but then its fields are reordered
    to all match the order of the channels in the recording .bin, so by channel,
    not physical contact layout. The returned probe object is ordered by raw channel.
    So returned probe data corresponds to raw .bin chan i.

    If things are shank sliced, then probe info should be in same order as in shank sliced rec data
    with device_channel_indices reset to local indices 0 to n-1.

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
    
    probe_dict = copy.deepcopy(probe_dict) # to avoid modifying original dict in place, since we will filter/reorder fields

    active_channels_mask = find_active_channels(probe_dict)
    probe = probe_dict['probes'][0]  # get the first probe

    # First filter by active channels
    for key in PROBE_ARRAY_KEYS:
        probe[key] = np.array(probe[key])[active_channels_mask]

    # Get the mapping to match here, converting json physical order to chan order
    sort_probe_arrays_by_device_chan(probe) 

    # Then filter by shank if specified
    n_channels = None
    if shank_num is not None:
        shank_channels_mask = find_shank_channels(probe, shank_num)
        for key in PROBE_ARRAY_KEYS:
            probe[key] = probe[key][shank_channels_mask]
        
        # Count actual channels from JSON after filtering
        n_channels = len(probe['device_channel_indices'])

        # If n chan shank was given make sure it makes sense
        if n_channels_shank is not None and n_channels_shank != n_channels:
            raise ValueError(
                f"your n_channels_shank {n_channels_shank} does not match count from JSON {n_channels} after filtering for shank {shank_num}."
            )

        # Once split rec into a shank, shank rec is locally indexed 0 to n-1, so make sure shank probe uses these indices too
        probe['device_channel_indices'] = np.arange(n_channels)

    probe = pi.Probe.from_dict(probe)
    return probe, n_channels
