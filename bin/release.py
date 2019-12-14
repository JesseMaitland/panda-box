import subprocess
import re
from pathlib import Path

# git branch | grep \* | cut -d ' ' -f2"

# args = ['git', 'branch', '|', 'grep', '\*', '|', 'cut', '-d', '" "', '-f2']
project_root = Path("/Users/jessemaitland/PycharmProjects/pandabox")
setup_path = project_root / "setup.py"

def run_process(*args):

    process = subprocess.Popen(args=args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=project_root)
    output, error = process.communicate()

    if error:
        print(error.decode("utf-8"))
        exit()
    else:
        return output, error


def split_output(output):
    return [x for x in output.decode('utf-8').split("\n") if x]


def is_on_master_branch():
    args = ['git', 'branch']
    current_branch = None

    output, error = run_process(*args)
    output = split_output(output)

    for line in output:
        if '*' in line:
            current_branch = line.replace('*', '').strip()


    if current_branch != 'master':
        print(f"current branch is {current_branch}, however you must be on master to deploy!")
        return False

    return True


def get_last_tag():
    args = ['git', 'tag', '--list']
    output, error = run_process(*args)
    output = split_output(output)

    if output:
        last_tag = output[-1]
    else:
        last_tag = '0.0.1'

    return last_tag


def increment_sub_version(tag):
    tag_parts = tag.split('.')
    tag_parts[-1] = str(int(tag_parts[-1]) + 1)
    tag = '.'.join(tag_parts)
    return tag


def create_git_tag(tag):
    args = ['git', 'tag', tag]
    output, error = run_process(*args)
    return True

def set_version_in_setup(tag):
    pattern = "VERSION = '[0-9]+.[0-9]+.[0-9]+'"
    new_version = f"VERSION = '{tag}'"
    setup_py = setup_path.read_text()
    new_setup_py = re.sub(pattern, new_version, setup_py)
    setup_path.write_text(new_setup_py)
    print(f"setup.py updated to {new_version}")



def main():
    if not is_on_master_branch():
        exit()

    tag = get_last_tag()
    tag = increment_sub_version(tag)

    create_git_tag(tag)

    if tag == get_last_tag():
        print(f"git tag {tag} created successfully.")

    set_version_in_setup(tag)

if __name__ == '__main__':
    main()
