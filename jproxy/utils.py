import xml.etree.ElementTree as ET

def remove_prefix(client_prefix, key):
    if key and client_prefix:
        return key.removeprefix(client_prefix).removeprefix('/')
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
    return ET.tostring(elem, encoding="utf-8", xml_declaration=True)


def parse_xml(xml):
    return ET.fromstring(xml)
