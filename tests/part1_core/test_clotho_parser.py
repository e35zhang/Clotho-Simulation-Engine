# tests/test_clotho_parser.py
"""
Comprehensive unit tests for the Clotho Parser (core/clotho_parser.py).

Tests cover:
- Valid blueprint loading
- Missing required keys
- Invalid key types
- YAML syntax errors
- File I/O errors
- Edge cases (empty files, encoding issues)
- Topology validation
- Error message quality
"""

import unittest
import tempfile
import os
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.engine.clotho_parser import load_clotho_from_file, ClothoValidationError, _validate_clotho_structure


class TestClothoParserValidBlueprints(unittest.TestCase):
    """Test parsing of valid, well-formed Clotho blueprints."""
    
    def setUp(self):
        """Create a temporary directory for test files."""
        self.test_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up temporary files."""
        for file in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, file))
        os.rmdir(self.test_dir)
    
    def _create_test_file(self, filename, content):
        """Helper to create a test YAML file."""
        filepath = os.path.join(self.test_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return filepath
    
    def test_minimal_valid_blueprint(self):
        """Test loading a minimal valid Clotho blueprint."""
        yaml_content = """
types: {}
design:
  components: []
test:
  invariants: []
run:
  scenarios: []
"""
        filepath = self._create_test_file('minimal.yaml', yaml_content)
        data = load_clotho_from_file(filepath)
        
        self.assertIsNotNone(data)
        self.assertIsInstance(data['types'], dict)
        self.assertIsInstance(data['design'], dict)
        self.assertIsInstance(data['test'], dict)
        self.assertIsInstance(data['run'], dict)
    
    def test_complete_valid_blueprint(self):
        """Test loading a complete valid Clotho blueprint with all sections."""
        yaml_content = """
types:
  messages:
    TestMessage:
      field1: string
design:
  components:
    - name: ComponentA
      state:
        - name: users
          schema:
            id: {type: string, pk: true}
      handlers: []
    - name: ComponentB
      state: []
      handlers: []
test:
  invariants: []
run:
  scenarios:
    - name: TestScenario
      initial_state: []
      steps: []
"""
        filepath = self._create_test_file('complete.yaml', yaml_content)
        data = load_clotho_from_file(filepath)
        
        self.assertIsNotNone(data)
        self.assertIsInstance(data['types'], dict)
        self.assertIsInstance(data['design'], dict)
        self.assertEqual(len(data['design']['components']), 2)
        self.assertEqual(data['design']['components'][0]['name'], 'ComponentA')
    
    def test_blueprint_with_unicode_characters(self):
        """Test loading blueprints with Unicode characters in content."""
        yaml_content = """
types: {}  # Unicode test: 系统架构
design:
  components:
    - name: Component_Unicode
      state: []
      handlers: []
test:
  invariants: []
run:
  scenarios: []
"""
        filepath = self._create_test_file('unicode.yaml', yaml_content)
        data = load_clotho_from_file(filepath)
        
        self.assertIsNotNone(data)
        self.assertIsInstance(data['design'], dict)
        self.assertEqual(data['design']['components'][0]['name'], 'Component_Unicode')
    
    def test_blueprint_with_empty_collections(self):
        """Test blueprints with empty but valid collections."""
        yaml_content = """
types: {}
design:
  components: []
test:
  invariants: []
run:
  scenarios: []
"""
        filepath = self._create_test_file('empty_collections.yaml', yaml_content)
        data = load_clotho_from_file(filepath)
        
        self.assertIsNotNone(data)
        self.assertEqual(len(data['design']['components']), 0)


class TestClothoParserMissingKeys(unittest.TestCase):
    """Test error handling for missing required keys."""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        for file in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, file))
        os.rmdir(self.test_dir)
    
    def _create_test_file(self, filename, content):
        filepath = os.path.join(self.test_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return filepath
    
    def test_missing_types(self):
        """Test error when types section is missing."""
        yaml_content = """
design:
  components: []
test:
  invariants: []
run:
  scenarios: []
"""
        filepath = self._create_test_file('no_types.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('types', str(cm.exception))
        self.assertIn('missing required section', str(cm.exception))
    
    def test_missing_design(self):
        """Test error when design section is missing."""
        yaml_content = """
types: {}
test:
  invariants: []
run:
  scenarios: []
"""
        filepath = self._create_test_file('no_design.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('design', str(cm.exception))
    
    def test_missing_test(self):
        """Test error when test section is missing."""
        yaml_content = """
types: {}
design:
  components: []
run:
  scenarios: []
"""
        filepath = self._create_test_file('no_test.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('test', str(cm.exception))
    
    def test_missing_run(self):
        """Test error when run section is missing."""
        yaml_content = """
types: {}
design:
  components: []
test:
  invariants: []
"""
        filepath = self._create_test_file('no_run.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('run', str(cm.exception))
    
    def test_missing_multiple_keys(self):
        """Test error message when multiple keys are missing."""
        yaml_content = """
types: {}
"""
        filepath = self._create_test_file('minimal_broken.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        error_msg = str(cm.exception)
        # Should mention multiple missing sections
        self.assertIn('design', error_msg)
        self.assertIn('test', error_msg)
        self.assertIn('run', error_msg)
    
    def test_missing_design_components(self):
        """Test error when design.components is missing."""
        yaml_content = """
types: {}
design:
  name: TestSystem
test:
  invariants: []
run:
  scenarios: []
"""
        filepath = self._create_test_file('no_design_components.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('design', str(cm.exception))
        self.assertIn('components', str(cm.exception))


class TestClothoParserInvalidTypes(unittest.TestCase):
    """Test error handling for invalid data types."""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        for file in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, file))
        os.rmdir(self.test_dir)
    
    def _create_test_file(self, filename, content):
        filepath = os.path.join(self.test_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return filepath
    
    def test_design_not_dict(self):
        """Test error when design is not a dictionary."""
        yaml_content = """
types: {}
design: []  # Should be dict, not list
test:
  invariants: []
run:
  scenarios: []
"""
        filepath = self._create_test_file('design_list.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('design', str(cm.exception))
        self.assertIn('dictionary', str(cm.exception))
    
    def test_design_components_not_list(self):
        """Test error when design.components is not a list."""
        yaml_content = """
types: {}
design:
  components: {}  # Should be list, not dict
test:
  invariants: []
run:
  scenarios: []
"""
        filepath = self._create_test_file('design_components_dict.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('components', str(cm.exception))
        self.assertIn('list', str(cm.exception))
    
    def test_types_not_dict(self):
        """Test error when types is not a dict."""
        yaml_content = """
types: []  # Should be dict, not list
design:
  components: []
test:
  invariants: []
run:
  scenarios: []
"""
        filepath = self._create_test_file('types_list.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('types', str(cm.exception))
        self.assertIn('dictionary', str(cm.exception))
    
    def test_run_not_dict(self):
        """Test error when run is not a dict."""
        yaml_content = """
types: {}
design:
  components: []
test:
  invariants: []
run: []  # Should be dict, not list
"""
        filepath = self._create_test_file('run_list.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('run', str(cm.exception))
        self.assertIn('dictionary', str(cm.exception))


class TestClothoParserYAMLSyntaxErrors(unittest.TestCase):
    """Test handling of YAML syntax errors."""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        for file in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, file))
        os.rmdir(self.test_dir)
    
    def _create_test_file(self, filename, content):
        filepath = os.path.join(self.test_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return filepath
    
    def test_invalid_yaml_syntax(self):
        """Test error on invalid YAML syntax."""
        yaml_content = """
clotho_version: '1.0'
design:
  components: [
components: []  # Unclosed bracket above
scenarios: []
"""
        filepath = self._create_test_file('bad_syntax.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('parsing YAML', str(cm.exception))
    
    def test_invalid_indentation(self):
        """Test error on invalid YAML indentation."""
        yaml_content = """
clotho_version: '1.0'
design:
components: []  # Wrong indentation
  scenarios: []
"""
        filepath = self._create_test_file('bad_indent.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        # Should catch either syntax error or validation error
        self.assertTrue(isinstance(cm.exception, ClothoValidationError))
    
    def test_unquoted_special_characters(self):
        """Test handling of YAML special characters without quotes."""
        # YAML treats some characters specially (: { } [ ] , & * # ? | - < > = ! % @ `)
        yaml_content = """
clotho_version: '1.0'
design:
  name: System: With: Colons
  components: []
components: []
scenarios: []
"""
        filepath = self._create_test_file('special_chars.yaml', yaml_content)
        
        # This should either parse successfully or raise syntax error
        try:
            data = load_clotho_from_file(filepath)
            # If it parses, check the result
            self.assertIsNotNone(data)
        except ClothoValidationError as e:
            # If it fails, it should be a YAML parsing error
            self.assertIn('parsing YAML', str(e))


class TestClothoParserFileErrors(unittest.TestCase):
    """Test handling of file I/O errors."""
    
    def test_file_not_found(self):
        """Test error when file doesn't exist."""
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file('/nonexistent/path/to/file.yaml')
        
        self.assertIn('could not be found', str(cm.exception))
        self.assertIn('/nonexistent/path/to/file.yaml', str(cm.exception))
    
    def test_empty_file_path(self):
        """Test handling of empty file path."""
        result = load_clotho_from_file('')
        self.assertIsNone(result)
        
        result = load_clotho_from_file(None)
        self.assertIsNone(result)
    
    def test_directory_instead_of_file(self):
        """Test error when path points to directory."""
        test_dir = tempfile.mkdtemp()
        try:
            with self.assertRaises((ClothoValidationError, IsADirectoryError, PermissionError)):
                load_clotho_from_file(test_dir)
        finally:
            os.rmdir(test_dir)


class TestClothoParserEdgeCases(unittest.TestCase):
    """Test edge cases and corner scenarios."""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        for file in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, file))
        os.rmdir(self.test_dir)
    
    def _create_test_file(self, filename, content):
        filepath = os.path.join(self.test_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return filepath
    
    def test_empty_file(self):
        """Test error on completely empty file."""
        filepath = self._create_test_file('empty.yaml', '')
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('empty', str(cm.exception).lower())
    
    def test_file_with_only_comments(self):
        """Test error on file with only YAML comments."""
        yaml_content = """
# This is a comment
# Another comment
# No actual content
"""
        filepath = self._create_test_file('only_comments.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('empty', str(cm.exception).lower())
    
    def test_file_with_null_document(self):
        """Test error on YAML file containing only null."""
        yaml_content = "~\n"  # YAML null
        filepath = self._create_test_file('null.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        self.assertIn('empty', str(cm.exception).lower())
    
    def test_very_large_blueprint(self):
        """Test loading a large blueprint (performance test)."""
        # Generate a blueprint with many components
        components_yaml = []
        for i in range(100):
            comp_name = f"Component{i}"
            components_yaml.append(f"""    - name: {comp_name}
      state: []
      handlers: []""")
        
        yaml_content = f"""
types: {{}}
design:
  components:
{chr(10).join(components_yaml)}
test:
  invariants: []
run:
  scenarios: []
"""
        filepath = self._create_test_file('large.yaml', yaml_content)
        data = load_clotho_from_file(filepath)
        
        self.assertIsNotNone(data)
        self.assertEqual(len(data['design']['components']), 100)


class TestValidateClothoStructureFunction(unittest.TestCase):
    """Test the _validate_clotho_structure function directly."""
    
    def test_valid_structure(self):
        """Test validation passes for valid structure."""
        valid_data = {
            'types': {},
            'design': {
                'components': []
            },
            'test': {},
            'run': {
                'scenarios': []
            }
        }
        # Should not raise any exception
        _validate_clotho_structure(valid_data)
    
    def test_missing_key_detection(self):
        """Test detection of missing keys."""
        invalid_data = {
            'types': {},
            'design': {'components': []}
            # Missing 'test' and 'run'
        }
        with self.assertRaises(ClothoValidationError):
            _validate_clotho_structure(invalid_data)
    
    def test_wrong_type_detection(self):
        """Test detection of wrong types."""
        invalid_data = {
            'types': {},
            'design': [],  # Should be dict
            'test': {},
            'run': {'scenarios': []}
        }
        with self.assertRaises(ClothoValidationError):
            _validate_clotho_structure(invalid_data)
    
    def test_validation_with_extra_keys(self):
        """Test that validation allows extra keys (forward compatibility)."""
        data_with_extras = {
            'types': {},
            'design': {
                'components': [],
                'extra_field': 'value'
            },
            'test': {},
            'run': {'scenarios': []},
            'metadata': {'author': 'test'}  # Extra top-level key
        }
        # Should not raise - extra keys are allowed
        _validate_clotho_structure(data_with_extras)


class TestErrorMessageQuality(unittest.TestCase):
    """Test that error messages are clear and actionable."""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        for file in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, file))
        os.rmdir(self.test_dir)
    
    def _create_test_file(self, filename, content):
        filepath = os.path.join(self.test_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return filepath
    
    def test_error_message_includes_missing_key_names(self):
        """Test that error messages list the missing keys."""
        yaml_content = "types: {}"
        filepath = self._create_test_file('missing.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        error_msg = str(cm.exception)
        # Should mention missing sections
        self.assertIn('design', error_msg)
        self.assertIn('test', error_msg)
        self.assertIn('run', error_msg)
    
    def test_error_message_includes_file_path(self):
        """Test that file not found error includes the path."""
        fake_path = '/some/fake/path/blueprint.yaml'
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(fake_path)
        
        self.assertIn(fake_path, str(cm.exception))
    
    def test_error_message_describes_expected_type(self):
        """Test that type errors describe the expected type."""
        yaml_content = """
types: {}
design: 'not_a_dict'
test: {}
run:
  scenarios: []
"""
        filepath = self._create_test_file('wrong_type.yaml', yaml_content)
        
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(filepath)
        
        error_msg = str(cm.exception)
        self.assertIn('design', error_msg)
        self.assertIn('dictionary', error_msg.lower())


# Test runner
if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
