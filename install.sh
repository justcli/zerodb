#!/usr/bin/env bash

which python3 1>/dev/null 2>&1
if [ $? -ne 0 ];then
	echo "Currently only works with python3"
	exit 1
fi

import_path=`python3 -c "import sys;print(sys.path[-1] + '/zerodb')"`
cli_path="/usr/local/bin/"

# copy the zerodb.py as module
mkdir -p "$import_path" 1>/dev/null 2>&1
if [ $? -ne 0 ];then
	echo "$import_path is already there."
	echo -n "Overwrite it [y|n]:"
	read -r choice
	if [ -z "$choice" ] || [ "$choice" != "y" ]; then
		echo "Installation aborted."
		exit 1
	fi
fi
import_path=$import_path"/"

cp -f ./zerodb.py ./zdict.py ./zlist.py zint.py __init__.py "$import_path"
if [ $? -ne 0 ];then
	echo "Unable to copy files to ""$import_path"\
			 ". Try running the script as sudo e.g. > sudo ./install.sh"
	exit 1
fi
echo Files copied to "$import_path":
echo "  __init__.py"
echo "  zerodb.py"
echo "  zdict.py"
echo "  zlist.py"
echo "  zint.py"

# copy tddish as cli app to the cli_path
cp -f ./zerodb.py ./zerodb
chmod 777 ./zerodb
cp -f ./zerodb $cli_path
if [ $? -ne 0 ];then
	echo "Unable to copy files to "$cli_path\
	   	 ". Try running the script as sudo e.g. > sudo ./install.sh"
	exit 1
fi
echo "Files copied to ""$cli_path"":"
echo "  zerodb"
rm ./zerodb
echo "Installation done!."

