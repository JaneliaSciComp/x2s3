import pytest
from fastapi.testclient import TestClient

from x2s3.app import create_app
from x2s3.settings import Settings
from x2s3.utils import (
    get_nosuchkey_response,
    get_nosuchbucket_response, 
    get_error_response,
    parse_xml
)


class TestXSSPrevention:
    """Test cases to ensure HTML/XML special characters are properly escaped in XML responses."""

    def test_nosuchkey_response_escapes_html_brackets(self):
        """Test that get_nosuchkey_response escapes HTML brackets in key names."""
        malicious_key = '<script>alert("XSS")</script>'
        response = get_nosuchkey_response(malicious_key)
        
        # Response should not contain unescaped brackets
        content = response.body.decode()
        assert '<script>' not in content
        assert '</script>' not in content
        
        # Should contain escaped versions
        assert '&lt;script&gt;' in content
        assert '&lt;/script&gt;' in content
        
        # Should be valid XML
        root = parse_xml(content)
        assert root.find('Key').text == malicious_key

    def test_nosuchkey_response_escapes_ampersands(self):
        """Test that ampersands are properly escaped."""
        malicious_key = 'test&value&more'
        response = get_nosuchkey_response(malicious_key)
        
        content = response.body.decode()
        # Should contain escaped ampersands
        assert '&amp;' in content
        # Should not contain unescaped ampersands (except in escape sequences)
        unescaped_amps = content.replace('&amp;', '').replace('&lt;', '').replace('&gt;', '').replace('&quot;', '').replace('&apos;', '')
        assert '&' not in unescaped_amps

    def test_nosuchbucket_response_escapes_html_brackets(self):
        """Test that get_nosuchbucket_response escapes HTML brackets in bucket names."""
        malicious_bucket = '<img src=x onerror=alert("XSS")>'
        response = get_nosuchbucket_response(malicious_bucket)
        
        content = response.body.decode()
        assert '<img' not in content
        # Note: HTML escaping converts quotes to &quot; so onerror= becomes onerror=
        # The key test is that < and > are escaped
        assert '&lt;img' in content
        assert '&gt;' in content
        
        # Should be valid XML
        root = parse_xml(content)
        assert root.find('BucketName').text == malicious_bucket

    def test_error_response_escapes_all_parameters(self):
        """Test that get_error_response escapes all user-provided parameters."""
        malicious_code = '<script>alert("code")</script>'
        malicious_message = '</Message><script>alert("msg")</script><Message>'
        malicious_resource = '<iframe src="javascript:alert(1)"></iframe>'
        
        response = get_error_response(400, malicious_code, malicious_message, malicious_resource)
        
        content = response.body.decode()
        
        # None of the malicious content should be unescaped
        assert '<script>' not in content
        assert '</script>' not in content  
        assert '<iframe' not in content
        assert '</Message><script>' not in content
        # Note: javascript: is OK in escaped content as &quot;javascript:alert(1)&quot;
        # The key is that the dangerous tags are escaped
        
        # Should contain properly escaped versions
        assert '&lt;script&gt;' in content
        assert '&lt;/script&gt;' in content
        assert '&lt;iframe' in content
        
        # Should be valid XML
        root = parse_xml(content)
        assert root.find('Code').text == malicious_code
        assert root.find('Message').text == malicious_message
        assert root.find('Resource').text == malicious_resource

    def test_quotes_are_escaped_in_xml_responses(self):
        """Test that quotes are properly escaped in XML responses."""
        key_with_quotes = 'test"value\'more'
        response = get_nosuchkey_response(key_with_quotes)
        
        content = response.body.decode()
        # Should be valid XML even with quotes
        root = parse_xml(content)
        assert root.find('Key').text == key_with_quotes


    def test_direct_xss_injection_in_bucket_name(self):
        """Test XSS injection directly through function calls with bucket names."""
        malicious_bucket = '<script>alert("XSS")</script>'
        response = get_nosuchbucket_response(malicious_bucket)
        
        content = response.body.decode()
        assert '<script>' not in content
        assert '&lt;script&gt;' in content

    def test_direct_xss_injection_in_error_messages(self):
        """Test XSS injection directly through function calls with error data."""
        malicious_error = '<img src=x onerror=alert(1)>'
        response = get_error_response(400, 'TestError', malicious_error, '/test')
        
        content = response.body.decode()
        assert '<img' not in content
        assert '&lt;img' in content

    def test_complex_xml_injection_attempt(self):
        """Test complex XML injection attempts are properly escaped."""
        # Attempt to inject additional XML elements
        malicious_input = '</Key><Script>alert("XSS")</Script><Key>'
        
        response = get_nosuchkey_response(malicious_input)
        content = response.body.decode()
        
        # Should not create additional XML elements
        assert '<script>' not in content.lower()
        assert '</script>' not in content.lower()
        
        # Should be properly escaped
        assert '&lt;/key&gt;' in content.lower()
        assert '&lt;script&gt;' in content.lower()
        
        # XML should still be well-formed with only expected structure
        root = parse_xml(content)
        assert len(root.findall('.//Key')) == 1  # Should only have one Key element
        assert root.find('Key').text == malicious_input

    def test_unicode_and_special_characters(self):
        """Test that unicode and special characters are handled safely."""
        # Only test with valid XML characters - XML 1.0 doesn't allow control chars
        unicode_chars = 'test\u2603\U0001F600'  # snowman and smiley emoji
        
        # This should not cause XML parsing errors
        response = get_nosuchkey_response(unicode_chars)
        
        # Should produce valid XML
        content = response.body.decode()
        
        # Should be parseable as XML
        root = parse_xml(content)
        
        # Unicode should be preserved
        assert root.find('Key').text == unicode_chars

    def test_xml_control_characters_are_handled(self):
        """Test that XML control characters don't break responses."""
        special_chars = 'test\ttab\nnewline\rcarriage'  # Valid XML whitespace chars
        
        response = get_nosuchkey_response(special_chars)
        content = response.body.decode()
        
        # Remove leading whitespace that inspect.cleandoc might add
        content = content.strip()
        
        # Should be parseable as XML
        root = parse_xml(content)
        # XML normalizes whitespace in text content, so just check that it parses and contains the key
        key_text = root.find('Key').text
        assert 'test' in key_text
        assert 'tab' in key_text
        assert 'newline' in key_text
        assert 'carriage' in key_text