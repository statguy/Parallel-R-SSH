#!/bin/sh
# Check if a git repository is up-to-date
# Portions taken from:
# http://stackoverflow.com/questions/2180270/check-if-current-directory-is-a-git-repository
# http://stackoverflow.com/questions/15715825/how-do-you-get-git-repos-name-in-some-git-repository
# http://stackoverflow.com/questions/3258243/git-check-if-pull-needed

if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "No git repository found in this directory"
    exit -2
fi

NAME=$(basename `git rev-parse --show-toplevel`)
LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse @{u})
BASE=$(git merge-base @ @{u})

if [ $LOCAL = $REMOTE ]; then
    echo "Repository $NAME up-to-date"
    exit 0
elif [ $LOCAL = $BASE ]; then
    echo "Repository $NAME need to pull"
elif [ $REMOTE = $BASE ]; then
    echo "Repository $NAME need to push"
else
    echo "Repository $NAME diverged"
fi

exit -1
