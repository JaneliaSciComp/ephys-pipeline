import json
import numpy as np
import probeinterface as pi

def load_probe_from_json(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    probe = pi.Probe(ndim=2, si_units='um',)

    positions = np.column_stack((data['xcoords'], data['ycoords']))
    probe.set_contacts(positions=positions, shapes='square', shape_params={'width': 12})
    device_channel_indices = data['chanMap']
    probe.set_device_channel_indices(device_channel_indices)
    probe.set_shank_ids(data['shankInd'])
    probe.create_auto_shape()
    
    return probe, data