def get_git_hash():
    try:
        import git
        repo = git.Repo(search_parent_directories=True)
        sha = repo.head.commit.hexsha
        return repo.git.rev_parse(sha, short=4)
    except Exception:
        return 'unknown'
