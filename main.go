package main

import (
	"fmt"
	"log"
	"math/big"
	"reflect"

	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func main() {

	// Set here the address of the OLD cluster:
	client_old, err := NewClient(StringToUint128("0"), []string{"3000"})
	if err != nil {
		log.Fatalf("Error creating client old: %s", err)
	}
	defer client_old.Close()

	// Set here the address of the NEW cluster:
	client_new, err := NewClient(StringToUint128("0"), []string{"3001"})
	if err != nil {
		log.Fatalf("Error creating client new: %s", err)
	}
	defer client_old.Close()

	fmt.Println("Importing accounts ...")
	account_timestamp_previous := uint64(0)
	account_last, err := client_new.QueryAccounts(QueryFilter{Limit: 1, Flags: QueryFilterFlags{Reversed: true}.ToUint32()})
	if err != nil {
		log.Fatalf("Error querying last imported account: %s", err)
	}
	if len(account_last) == 1 {
		account_timestamp_previous = account_last[0].Timestamp + 1
	}

	accounts_exported_count := 0
	for {
		accounts, err := client_old.QueryAccounts(QueryFilter{
			TimestampMin: uint64(account_timestamp_previous),
			Limit:        8190,
		})
		if err != nil {
			log.Fatalf("Error loading accounts: %s", err)
		}

		if len(accounts) == 0 {
			break
		}

		for i := 0; i < len(accounts); i++ {
			// Zeroing the balance.
			accounts[i].DebitsPending = ToUint128(0)
			accounts[i].DebitsPosted = ToUint128(0)
			accounts[i].CreditsPending = ToUint128(0)
			accounts[i].CreditsPosted = ToUint128(0)
			// Setting the imported flag.
			flags := accounts[i].AccountFlags()
			flags.Imported = true
			// Setting the linked (except for the last element).
			flags.Linked = i != len(accounts)-1
			accounts[i].Flags = flags.ToUint16()

			accounts_exported_count += 1
		}

		results, err := client_new.CreateAccounts(accounts)
		if err != nil {
			log.Fatalf("Error creating accounts: %s\nLast timestamp was %d", err, account_timestamp_previous)
		}

		if len(results) != 0 {
			fmt.Println("Account failed:")
			for _, result := range results {
				if result.Result != AccountLinkedEventFailed {
					fmt.Printf("Result %d %s\n", result.Index, result.Result.String())
				}
			}
			log.Fatalf("Result creating accounts\nLast timestamp was %d", account_timestamp_previous)
		}

		account_timestamp_previous = accounts[len(accounts)-1].Timestamp + 1
	}

	fmt.Println("Importing transfers ...")
	transfer_timestamp_previous := uint64(0)
	transfer_last, err := client_new.QueryTransfers(QueryFilter{Limit: 1, Flags: QueryFilterFlags{Reversed: true}.ToUint32()})
	if err != nil {
		log.Fatalf("Error querying last imported transfer: %s", err)
	}
	if len(transfer_last) == 1 {
		transfer_timestamp_previous = transfer_last[0].Timestamp + 1
	}

	transfers_exported_count := uint(0)
	for {
		transfers, err := client_old.QueryTransfers(QueryFilter{
			TimestampMin: uint64(transfer_timestamp_previous),
			Limit:        8190,
		})
		if err != nil {
			log.Fatalf("Error loading transfers: %s", err)
		}

		if len(transfers) == 0 {
			break
		}

		for i := 0; i < len(transfers); i++ {
			flags := transfers[i].TransferFlags()
			// Setting the imported flag.
			flags.Imported = true
			// Setting the linked (except for the last element).
			flags.Linked = i != len(transfers)-1
			transfers[i].Flags = flags.ToUint16()

			transfers_exported_count += 1
		}

		results, err := client_new.CreateTransfers(transfers)
		if err != nil {
			log.Fatalf("Error creating transfers: %snLast timestamp was %d", err, transfer_timestamp_previous)
		}

		if len(results) != 0 {
			fmt.Println("Transfer failed:")
			for _, result := range results {
				if result.Result != TransferLinkedEventFailed {
					fmt.Printf("Result %d %s\n", result.Index, result.Result.String())
				}
			}
			log.Fatalf("Result creating transfers\nLast timestamp was %d", transfer_timestamp_previous)
		}

		transfer_timestamp_previous = transfers[len(transfers)-1].Timestamp + 1
	}

	fmt.Println("Validating balances ...")
	account_timestamp_previous = 0
	for {
		accounts_old, err := client_old.QueryAccounts(QueryFilter{
			TimestampMin: account_timestamp_previous,
			Limit:        8190,
		})
		if err != nil {
			log.Fatalf("Error loading old accounts: %s", err)
		}

		accounts_new, err := client_new.QueryAccounts(QueryFilter{
			TimestampMin: account_timestamp_previous,
			Limit:        8190,
		})
		if err != nil {
			log.Fatalf("Error loading new accounts: %s", err)
		}

		if len(accounts_old) != len(accounts_new) {
			log.Fatalf("Error accounts length differ: %d -> %d", len(accounts_old), len(accounts_new))
		}
		if len(accounts_old) == 0 {
			break
		}
		account_timestamp_previous = accounts_old[len(accounts_old)-1].Timestamp + 1

		for i := 0; i < len(accounts_old); i++ {
			// Asserting the balance before and after:
			equals :=
				accounts_new[i].Timestamp == accounts_old[i].Timestamp &&
					reflect.DeepEqual(accounts_new[i].DebitsPending.Bytes(), accounts_old[i].DebitsPending.Bytes()) &&
					reflect.DeepEqual(accounts_new[i].DebitsPosted.Bytes(), accounts_old[i].DebitsPosted.Bytes()) &&
					reflect.DeepEqual(accounts_new[i].CreditsPending.Bytes(), accounts_old[i].CreditsPending.Bytes()) &&
					reflect.DeepEqual(accounts_new[i].CreditsPosted.Bytes(), accounts_old[i].CreditsPosted.Bytes())
			if !equals {
				log.Fatalf("Error accounts differ: %d -> %d", accounts_new[i].Timestamp, accounts_old[i].Timestamp)
			}
		}
	}

	fmt.Printf("Exported accounts: %d\nExported transfers: %d\n", accounts_exported_count, transfers_exported_count)
	fmt.Println("Finished")
}

func StringToUint128(str string) Uint128 {
	clusterIdBigInt := new(big.Int)
	clusterIdBigInt.SetString(str, 10)
	clusterId := BigIntToUint128(*clusterIdBigInt)
	return clusterId
}
