# core/clotho_parser.py
#
# This module is responsible for loading and validating the Clotho (Clotho Definition
# Language) YAML file. It acts as a gateway to ensure that the
# rest of the application deals with well-structured data.

import yaml

class ClothoValidationError(Exception):
    """Custom exception for errors during Clotho file validation."""
    pass

def _validate_clotho_structure(data):
    """
    Validates the Clotho structure (Design, Types, Test, Run).
    Only supports the modern Clotho format.
    Raises ClothoValidationError with a specific message if any check fails.
    """
    # Enforce modern Clotho structure
    required_sections = ['design', 'types', 'test', 'run']
    missing_sections = [section for section in required_sections if section not in data]
    
    if missing_sections:
        raise ClothoValidationError(f"Clotho file is missing required section(s): {', '.join(missing_sections)}")
    
    # Validate Design section
    design = data['design']
    if not isinstance(design, dict):
        raise ClothoValidationError("The 'design' section must be a dictionary.")
    if 'components' not in design:
        raise ClothoValidationError("The 'design' section is missing the required 'components' key.")
    if not isinstance(design['components'], list):
        raise ClothoValidationError("The 'design.components' must be a list.")
    
    # Validate Types section
    if not isinstance(data['types'], dict):
        raise ClothoValidationError("The 'types' section must be a dictionary.")
    
    # Validate Test section
    if not isinstance(data['test'], dict):
        raise ClothoValidationError("The 'test' section must be a dictionary.")
    
    # Validate Run section
    run = data['run']
    if not isinstance(run, dict):
        raise ClothoValidationError("The 'run' section must be a dictionary.")
    if 'generators' not in run and 'scenarios' not in run:
        raise ClothoValidationError("The 'run' section must contain either 'generators' or 'scenarios'.")

def load_clotho_from_file(file_path):
    """
    Loads and validates a Clotho blueprint from a file path.

    It checks for YAML syntax correctness and performs deep structural validation
    to ensure the data conforms to the required schema.

    :param file_path: A string containing the path to the YAML file.
    :return: A Python dictionary containing the parsed Clotho data.
    :raises ClothoValidationError: If the file is not found, poorly formatted, or fails validation.
    """
    if not file_path:
        return None
        
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        raise ClothoValidationError(f"The file could not be found at path: {file_path}")
    except yaml.YAMLError as e:
        raise ClothoValidationError(f"Error parsing YAML file: {e}")
    except UnicodeDecodeError as e:
        raise ClothoValidationError(f"Failed to decode file using UTF-8 (Check file encoding): {e}")

    # Check for None in case the YAML file is empty or just contains comments
    if data is None:
        raise ClothoValidationError("The YAML file is empty or invalid.")

    # Validate Clotho structure
    _validate_clotho_structure(data)
    
    return data 
