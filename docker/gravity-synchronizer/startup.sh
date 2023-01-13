#!/bin/bash

get_args() {
	_arg=$1
	shift
	echo "$@" | grep -Eo "\-\-${_arg}=[^ ]+" | cut -d= -f2 | tail -n 1
	unset _arg
}

stores=$(get_args stores "$@")

storeConfigPath="./rules/store.json"

[ "$GRAVITY_SYNCHRONIZER_RULES_STORE" != "" ] && {
	storeConfigPath=$GRAVITY_SYNCHRONIZER_RULES_STORE
} 

[ "$stores" != "" ] && {
	echo $stores > $storeConfigPath
}
[ "$stores" = "" ] && {
	echo $GRAVITY_SYNCHRONIZER_STORE_SETTINGS > $storeConfigPath
}

rules=$(get_args rules "$@")

rulesConfigPath="./rules/rules.json"

[ "$rules" != "" ] && {
	echo $rules > $rulesConfigPath
}
[ "$rules" = "" ] && {
	echo $GRAVITY_SYNCHRONIZER_RULES_SETTINGS > $rulesConfigPath
}

rules=$(get_args rules "$@")

rulesConfigPath="./rules/rules.json"

[ "$rules" != "" ] && {
	echo $rules > $rulesConfigPath
}
[ "$rules" = "" ] && {
	echo $GRAVITY_SYNCHRONIZER_RULES_SETTINGS > $rulesConfigPath
}

collection=$(get_args collection "$@")

collectionConfigPath="./rules/collection.json"

[ "$collection" != "" ] && {
	echo $collection > $collectionConfigPath
}
[ "$collection" = "" ] && {
	echo $GRAVITY_SYNCHRONIZER_COLLECTION_SETTINGS > $collectionConfigPath
}

exec /gravity-synchronizer
