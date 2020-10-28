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

		[ "$GRAVITY_SYNCHRONIZER_RULES_STORE" != "" ] && {
			storeConfigPath=$GRAVITY_SYNCHRONIZER_RULES_STORE
		} 

		echo $stores > $storeConfigPath
	}

	triggers=$(get_args triggers "$@")
	[ "$triggers" != "" ] && {
		triggerConfigPath="./rules/trigger.json"

		[ "$GRAVITY_SYNCHRONIZER_RULES_TRIGGER" != "" ] && {
			triggerConfigPath=$GRAVITY_SYNCHRONIZER_RULES_TRIGGER
		} 

		echo $triggers > $triggerConfigPath
	}

	transmitter=$(get_args transmitter "$@")
	[ "$transmitter" != "" ] &&{
		transmitterConfigPath="./rules/transmitter.json"

		[ "$GRAVITY_SYNCHRONIZER_RULES_TRANSMITTER" != "" ] && {
			transmitterConfigPath=$GRAVITY_SYNCHRONIZER_RULES_TRANSMITTER
		} 

		echo $transmitter > $transmitterConfigPath
	}

	exporter=$(get_args exporter "$@")
	[ "$exporter" != "" ] &&{
		exporterConfigPath="./rules/exporter.json"

		[ "$GRAVITY_SYNCHRONIZER_RULES_EXPORTER" != "" ] && {
			exporterConfigPath=$GRAVITY_SYNCHRONIZER_RULES_EXPORTER
		} 

		echo $exporter > $exporterConfigPath
	}
}
export GRAVITY_SYNCHRONIZER_EVENT_STORE_CLIENT_NAME=$(hostname)
exec /gravity-synchronizer
