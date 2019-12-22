from unittest import TestCase
from pandas import DataFrame
from pandabox.tools import Panda
from pandabox.tools import PandaBox


class TestPandaBox(TestCase):

    def setUp(self) -> None:
        self.panda_foo = Panda(name='foo', data=DataFrame())
        self.panda_spam = Panda(name='spam', data=DataFrame())
        self.panda_box = PandaBox(self.panda_foo, self.panda_spam)

    def test_get_panda(self):
        panda_foo = self.panda_box.get_panda('foo')
        self.assertEqual(panda_foo, self.panda_foo)
        self.assertIsInstance(panda_foo, Panda)

    def test_add_panda(self):
        new_panda = Panda(name='new', data=DataFrame())
        self.panda_box.add_panda(new_panda)
        names = self.panda_box.panda_names
        self.assertTrue('new' in names)

    def test_remove_panda(self):
        panda_spam = self.panda_box.remove_panda('spam')
        names = self.panda_box.panda_names
        self.assertIsInstance(panda_spam, Panda)
        self.assertEqual(panda_spam, self.panda_spam)
        self.assertTrue('spam' not in names)

    def test_destroy_panda(self):
        self.panda_box.destroy_panda('spam')
        names = self.panda_box.panda_names
        self.assertTrue('spam' not in names)
