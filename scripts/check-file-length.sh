#!/usr/bin/env sh
# Fails when an authored source file reaches 500 lines (vendored web assets excluded). Lists the
# git-tracked files, or every file outside target/ in a tree without git (the public split).
set -eu
files() {
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git ls-files '*.rs' '*.sh' '*.yml' '*.yaml' '*.html' '*.js' '*.css'
  else
    find . -path ./target -prune -o -type f \( -name '*.rs' -o -name '*.sh' -o -name '*.yml' \
      -o -name '*.yaml' -o -name '*.html' -o -name '*.js' -o -name '*.css' \) -print | sed 's|^\./||'
  fi
}
bad=0
n_files=0
for f in $(files | grep -v '^web/vendor/'); do
  n_files=$((n_files + 1))
  n=$(wc -l < "$f")
  if [ "$n" -ge 500 ]; then echo "too long ($n lines): $f"; bad=1; fi
done
[ "$n_files" -gt 0 ] || { echo "no source files found"; exit 1; }
exit $bad
