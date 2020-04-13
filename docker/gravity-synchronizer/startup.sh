#!/bin/bash

get_args() {
	_arg=$1
	shift
	echo "$@" | grep -Eo "\-\-${_arg}=[^ ]+" | cut -d= -f2 | tail -n 1
	unset _arg
}

[ "$#" -eq 0 ] || {
	stores=$(get_args stores "$@")
	[ "$stores" != "" ] && {
		storeConfigPath="./rules/stores.json"

		[ "$GRAVITY_SYNCHRONIZER_RULES_STORECONFIG" != "" ] && {
			storeConfigPath=$GRAVITY_SYNCHRONIZER_RULES_STORECONFIG
		} 

		echo $stores > $storeConfigPath
	}

	tiggers=$(get_args tiggers "$@")
	[ "$tiggers" != "" ] && {
		tiggerConfigPath="./rules/tiggers.json"

		[ "$GRAVITY_SYNCHRONIZER_RULES_TIGGERCONFIG" != "" ] && {
			tiggerConfigPath=$GRAVITY_SYNCHRONIZER_RULES_TIGGERCONFIG
		} 

		echo $tiggers > $tiggerConfigPath
	}

	db=$(get_args db "$@")
	[ "$db" != "" ] &&{
		dbConfigPath="./rules/database.json"

		[ "$GRAVITY_SYNCHRONIZER_RULES_DBCONFIG" != "" ] && {
			dbConfigPath=$GRAVITY_SYNCHRONIZER_RULES_DBCONFIG
		} 

		echo $db > $dbConfigPath
	}

	exporter=$(get_args exporter "$@")
	[ "$exporter" != "" ] &&{
		exporterConfigPath="./rules/exporter.json"

		[ "$GRAVITY_SYNCHRONIZER_RULES_EXPORTERCONFIG" != "" ] && {
			exporterConfigPath=$GRAVITY_SYNCHRONIZER_RULES_EXPORTERCONFIG
		} 

		echo $exporter > $exporterConfigPath
	}
}
export GRAVITY_SYNCHRONIZER_EVENT_STORE_CLIENT_NAME=$(hostname)
/gravity-synchronizer
