import sys
import types

from config.utils import get_git_hash


def test_get_git_hash_no_git(monkeypatch):
    # make importing 'git' raise ImportError
    real_import = __import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == 'git':
            raise ImportError
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr('builtins.__import__', fake_import)
    assert get_git_hash() == 'unknown'


def test_get_git_hash_with_fake_git(monkeypatch):
    # inject a fake git module with a Repo that returns a known sha
    mod = types.ModuleType('git')

    class DummyRepo:
        def __init__(self, search_parent_directories=True):
            self.head = types.SimpleNamespace(commit=types.SimpleNamespace(hexsha='abcd1234'))

            class GitObj:
                def rev_parse(self, sha, short=4):
                    return sha[:short]

            self.git = GitObj()

    mod.Repo = DummyRepo
    monkeypatch.setitem(sys.modules, 'git', mod)

    assert get_git_hash() == 'abcd'
