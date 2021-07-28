// Copyright 2021 Censored Planet

/* This program will fetch the latest domains and their categories from the citizen lab test list.
The input can be a list of domains, or a list of domans and old categories.
The output (printed on stdout) will be the unique input list of domains and their new categories,
or old categories/None if a new category could not be found.
*/

package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

//Collect all the CLTL categories from the different lists
func GetCitizenLabCategories() map[string]string {
	//Hardcode citizen Lab filenames
	//Calculated using cat 00-LEGEND-country_codes.csv | awk -F ',' '{print "\""tolower($1)"\""}' |  paste -s -d, -
	//Add global for the global list and prioritize global list categories.
	citizenLabFileQuantifiers := []string{"global", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "at", "au", "aw", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "bj", "bm", "bn", "bo", "br", "bs", "bt", "bu", "bv", "bw", "by", "bz", "ca", "cc", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "cr", "cs", "cu", "cv", "cx", "cy", "cz", "dd", "de", "dj", "dk", "dm", "do", "dz", "ec", "ee", "eg", "eh", "er", "es", "et", "fi", "fj", "fk", "fm", "fo", "fr", "fx", "ga", "gb", "gd", "ge", "gf", "gh", "gi", "gl", "gm", "gn", "gp", "gq", "gr", "gs", "gt", "gu", "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "in", "io", "iq", "ir", "is", "it", "jm", "jo", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky", "kz", "la", "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "mg", "mh", "ml", "mn", "mm", "mo", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "nf", "ng", "ni", "nl", "no", "np", "nr", "nt", "nu", "nz", "om", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pn", "pr", "pt", "pw", "py", "qa", "re", "ro", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "sk", "sl", "sm", "sn", "so", "sr", "st", "su", "sv", "sy", "sz", "tc", "td", "tf", "tg", "th", "tj", "tk", "tm", "tn", "to", "tp", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "um", "us", "uy", "uz", "va", "vc", "ve", "vg", "vi", "vn", "vu", "wf", "ws", "yd", "ye", "yt", "yu", "za", "zm", "zr", "zw"}
	citizenLabCategories := make(map[string]string)
	for _, code := range citizenLabFileQuantifiers {
		filename := []string{"https://raw.githubusercontent.com/citizenlab/test-lists/master/lists/", code, ".csv"}
		url := strings.Join(filename, "")
		resp, err := http.Get(url)
		if err != nil {
			log.Fatal("Error fetching Citizen Lab resource: ", err)
		}
		defer resp.Body.Close()
		reader := csv.NewReader(resp.Body)
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal("Error: ", err)
			}
			//All citizen lab entries begin with http or https
			if !strings.HasPrefix(record[0], "http") {
				continue
			}
			//Kinda hackish way to get the Subdomain + domain + TLD
			domain := strings.Split(record[0], "/")[2]
			if _, ok := citizenLabCategories[domain]; !ok {
				citizenLabCategories[domain] = record[2]
			}
			//Store the non-www version of the domain
			if strings.HasPrefix(domain, "www") {
				domainParts := strings.Split(domain, ".")
				domainWithoutSubdomain := domainParts[len(domainParts)-2] + "." + domainParts[len(domainParts)-1]
				if _, ok := citizenLabCategories[domainWithoutSubdomain]; !ok {
					citizenLabCategories[domainWithoutSubdomain] = record[2]
				}
			}
		}
	}
	return citizenLabCategories
}

//Match the domain with citizen lab categories, and print the result to stdout
func MatchCategories(domains map[string]string) {
	citizenLabCategories := GetCitizenLabCategories()
	for domain, oldCategory := range domains {
		if category, ok := citizenLabCategories[domain]; ok {
			fmt.Println(domain + "," + category)
			continue
		}
		//Note: Currently, I am not approximating since this may lead to errors.
		//If not able to find an exact match for a domain, see if an approximate can be found after removing the subdomain
		//domainParts := strings.Split(domain,".")
		//domainWithoutSubdomain := domainParts[len(domainParts)-2] + "." + domainParts[len(domainParts)-1]
		//if category, ok := citizenLabCategories[domainWithoutSubdomain]; ok {
		//	fmt.Println(domain + "," + category)
		//	continue
		//}

		//Try the www version of the domain, maybe that has a match?
		if !strings.HasPrefix(domain, "www") {
			wwwDomain := "www." + domain
			if category, ok := citizenLabCategories[wwwDomain]; ok {
				fmt.Println(domain + "," + category)
				continue
			}
		}

		//Try the non-www version of the domain, maybe that has a match?
		if strings.HasPrefix(domain, "www") {
			domainParts := strings.Split(domain, ".")
			domainWithoutSubdomain := domainParts[len(domainParts)-2] + "." + domainParts[len(domainParts)-1]
			if category, ok := citizenLabCategories[domainWithoutSubdomain]; ok {
				fmt.Println(domain + "," + category)
				continue
			}
		}

		//Return old category if not empty
		if oldCategory != "" {
			fmt.Println(domain + "," + oldCategory)
			continue
		}

		//Return none
		fmt.Println(domain + "," + "None")
	}
}

func main() {
	//Get the list of domains to categorize
	//Accept both just the list of domains and a list of domains + old categories that can be updated
	domainFile, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal("Could not open domains file")
	}
	defer domainFile.Close()
	domains := make(map[string]string)
	scanner := bufio.NewScanner(domainFile)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ",")
		if len(parts) == 2 {
			domains[parts[0]] = parts[1]
		} else {
			domains[parts[0]] = ""
		}

	}
	MatchCategories(domains)

}
