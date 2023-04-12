rm output.txt
touch output.txt
while read p; do
  echo $p
  echo https://$p
  echo $p >> output.txt
  curl -m 15 -w "\n%{http_code}\n" https://$p | tail -n 1 >> output.txt
done < all_sat_domains.csv