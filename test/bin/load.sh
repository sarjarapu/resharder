c=1
while [ $c -le 5 ]
do
	mongoimport --db test --collection grades --jsonArray --file ../import/records.json
	(( c++ ))
done
