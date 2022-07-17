#!/bin/bash
docker exec mysql mysql --user=mysql --password=mysql -e "USE states; SELECT * FROM states"
# docker exec mysql mysql --user=mysql --password=mysql -e "USE states; SELECT * FROM states WHERE timestamp > 2 and timestamp <= 5"
