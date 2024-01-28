import configparser

def UpdateConfig(filepath, section, option, value):
    """
    Updates a config file with the passed values.

    Parameters:
    - filepath (str): The absolue filepath of the config file.
    - section (str): The section in the config to update.
    - option (str): The item under the specified section to be updated.
    - value (str): Value to assign the item.
    """
    # Read the config and preserve the casing of items
    config = configparser.ConfigParser(interpolation=None)
    config.optionxform = str # type: ignore
    config.read_file(open(filepath))

    # Update the option with the new value
    config.set(section, option, value)

    # Save the changes back to the config file
    with open(filepath, 'w') as configfile:
        config.write(configfile)