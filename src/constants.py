from typing import Dict, List


def reverse_dict(dict_: Dict[str, List[str]]) -> Dict[str, str]:
    """Maps every value from the list to key, which stores the list.

    Maps every value from the list to key, which stores the list.
    Every value is converted to lowercase, before mapping.

    Examples:
      >>> reverse_dict({'k1': ['v1', 'V2'], 'k2': ['v3']})
      {'v1': 'k1', 'v2': 'k1', 'v3': 'k2'}
    """

    result = {}

    for key, values in dict_.items():
        values = [value.lower() for value in values]

        result.update(dict.fromkeys(values, key))

    return result


_UNITS = {
    "ft": ["f", "ft", "'", "feet"],
    "m": ["m", "m.", "meter", "meters"],
    "in": ["in", "inch", "inches", '"', "ins", "inus"],
    "mm": ["mm"],
    "ohmm": ["ohmm", "ohm.m", "ohm-"],
    "gAPI": ["gapi"],
    "ft3/ft3": ["cfcf"],
    "pu": ["pu"],
    "lbf": ["lbf"],
    "C": ["degc"],
    "F": ["degf"],
    "K": ["degk"],
    "mD": ["md"],
    "g/cm3": ["g/c3", "g/cc", "g/cm3"],
    "g/m3": ["gm/3", "gm/c"],
    "lbm/gal": ["ppg", "lb/g", "lbm/gal"],
    "ppm": ["ppm"],
    "mV": ["mv"],
    "ft/s": ["ft/s"],
    "m/s": ["m/s"],
    "kPa": ["kPa"],
    "psi": ["psi"],
    "bar": ["bar"],
    "us/ft": ["us/f", "us/ft"],
    "us/m": ["us/m"],
    "": ["none"],
}

_UNIT_BUCKETS = {
    'Length': _UNITS['m'] + _UNITS['ft'],
    'Gamma': _UNITS['gAPI'],
    'Resistivity': _UNITS['ohmm'],
    'Density': _UNITS['g/cm3'] + _UNITS['g/m3'] + _UNITS['lbm/gal'],
    'Porosity': _UNITS['ft3/ft3'] + _UNITS['pu'],
    'Permiability': _UNITS['mD'],
    'Acoustic Slowness': _UNITS['us/ft'] + _UNITS['us/m'],
    'Temperature': _UNITS['C'] + _UNITS['F'] + _UNITS['K'],
    'Short Length': _UNITS['mm'] + _UNITS['in'],
    'SP (Spontaneous Potential)': _UNITS['mV'],
    'Concentration': _UNITS['ppm'],
    'Velocity': _UNITS['m/s'] + _UNITS['ft/s'],
    'Pressure': _UNITS['kPa'] + _UNITS['bar'] + _UNITS['psi'],
    'Other': ['*'],
}

_MNEMONICS = {
    'md': ['dept', 'depth'],
}

UNITS = reverse_dict(dict_=_UNITS)
UNIT_BUCKETS = reverse_dict(dict_=_UNIT_BUCKETS)
MNEMONICS = reverse_dict(dict_=_MNEMONICS)
