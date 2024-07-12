import xml.etree.ElementTree as ET

def remove_prefix(prefix, key):
    """ Remove prefix from the key, and then the leading slash.
    """
    if key and prefix:
        return key.removeprefix(prefix).removeprefix('/')
    return key


def dir_path(path):
    """ Ensure that the given path ends in a slash, 
        indicating that it points to a folder and not an object.
    """
    if path and not path.endswith('/'):
        return path + '/'
    return path


def add_elem(parent, key):
    """ Add a new child element to the given XML parent.
    """
    return ET.SubElement(parent, key)


def add_telem(parent, key, value):
    """ Add a text element as a child of the given XML parent.
    """
    if not value: return None
    elem = add_elem(parent, key)
    elem.text = str(value)
    return elem


def elem_to_str(elem):
    """ Render the given XML element to a string.
    """
    return ET.tostring(elem, encoding="utf-8", xml_declaration=True)


def parse_xml(xml):
    """ Parse the given XML string into an XML element.
    """
    return ET.fromstring(xml)


# From https://stackoverflow.com/questions/1094841/get-a-human-readable-version-of-a-file-size
def humanize_bytes(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} Yi{suffix}"
