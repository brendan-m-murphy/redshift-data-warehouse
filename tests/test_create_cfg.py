from unittest import TestCase, main
from bin import create_cfg


class ReadCSVTestCase(TestCase):
    def test_fake_creds1(self):
        key, secret = create_cfg.read_cred_csv("tests/fake_creds1.csv")
        self.assertEqual(key, "AAAA")
        self.assertEqual(secret, "aaaa")

    def test_fake_creds2(self):
        key, secret = create_cfg.read_cred_csv("tests/fake_creds2.csv")
        self.assertEqual(key, "ABCD")
        self.assertEqual(secret, "EFGH")


if __name__ == '__main__':
    main()
