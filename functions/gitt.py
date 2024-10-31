from Functions.system import System
import git


class Git:

    @staticmethod
    def get_version():
        repo = git.Repo(System.get_project_path())
        branch = repo.active_branch.name
        last_commit = repo.commit(branch)
        hash_last_commit = last_commit.hexsha
        version_id = f'{branch}-{hash_last_commit[:7]}'
        return str(version_id)
