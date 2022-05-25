import os
from servicenow_api_tools.clients import utils
import unittest


class CredentailsTestCase(unittest.TestCase):
    def setUp(self):
        self._real_username = os.getenv("SNOW_USER")
        self._real_password = os.getenv("SNOW_PASS")
        os.unsetenv('SNOW_USER')
        os.unsetenv('SNOW_PASS')

    def tearDown(self):
        if self._real_username:
            os.environ['SNOW_USER'] = self._real_username
        if self._real_password:
            os.environ['SNOW_PASS'] = self._real_password

    def test_passed_credentials_implicit_override(self):
        os.environ['SNOW_USER'] = "wrong_username"
        os.environ['SNOW_PASS'] = "wrong_password"
        (username_out, password_out) = utils.load_credentials(
            username="right_username",
            password="right_password",
        )
        self.assertEqual(username_out, "right_username")
        self.assertEqual(password_out, "right_password")

    def test_passed_credentials_explicit_override(self):
        os.environ['SNOW_USER'] = "wrong_username"
        os.environ['SNOW_PASS'] = "wrong_password"
        (username_out, password_out) = utils.load_credentials(
            username="right_username",
            password="right_password",
            credentials_from_env=False,
        )
        self.assertEqual(username_out, "right_username")
        self.assertEqual(password_out, "right_password")

    def test_environment_credentials(self):
        os.environ['SNOW_USER'] = "right_username"
        os.environ['SNOW_PASS'] = "right_password"
        (username_out, password_out) = utils.load_credentials()
        self.assertEqual(username_out, "right_username")
        self.assertEqual(password_out, "right_password")

    def test_arg_username_env_password(self):
        os.environ['SNOW_USER'] = "wrong_username"
        os.environ['SNOW_PASS'] = "right_password"
        (username_out, password_out) = utils.load_credentials(
            username="right_username",
        )
        self.assertEqual(username_out, "right_username")
        self.assertEqual(password_out, "right_password")

    def test_env_username_arg_password(self):
        os.environ['SNOW_USER'] = "right_username"
        os.environ['SNOW_PASS'] = "wrong_password"
        (username_out, password_out) = utils.load_credentials(
            password="right_password",
        )
        self.assertEqual(username_out, "right_username")
        self.assertEqual(password_out, "right_password")

    def test_no_credentials(self):
        try:
            utils.load_credentials()
            self.fail("should fail if no credentials are found")
        except Exception:
            pass

    def test_no_username_env_password(self):
        try:
            os.environ['SNOW_PASS'] = "right_password"
            utils.load_credentials()
            self.fail("should fail if no username is found")
        except Exception:
            pass

    def test_no_username_arg_password(self):
        try:
            utils.load_credentials(password="right_password")
            self.fail("should fail if no username is found")
        except Exception:
            pass

    def test_env_username_no_password(self):
        try:
            os.environ['SNOW_USER'] = "right_username"
            utils.load_credentials()
            self.fail("should fail if no password is found")
        except Exception:
            pass

    def test_arg_username_no_password(self):
        try:
            utils.load_credentials(username="right_username")
            self.fail("should fail if no password is found")
        except Exception:
            pass
