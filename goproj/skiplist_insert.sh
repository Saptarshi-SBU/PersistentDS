for i in 10 100 1000 10000 100000 1000000;
	do
		go test -timeout 12000s -run TestSkiplistInsertValuesV2 -args -count=$i;
	done
