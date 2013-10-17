git add . -A
git commit -m "update"
git push
ssh labVM "cd /data/a/imru/ucscImru;git pull"
