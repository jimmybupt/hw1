./bin/impala-shell.sh -f test_01 -o tmp_nlj &> test.log

DIFF=$(diff -B tmp_hash tmp_nlj)

if [ "$DIFF" != "" ] 
then
    echo "Some test case faild, the details are\n"
    echo $DIFF
else
    echo "All test case passed!"
fi

