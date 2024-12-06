package main

import (
	"fmt"
	"log"
	"reflect"

	. "github.com/tigerbeetle/tigerbeetle-go"
	. "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

func main() {

	// Set here the address of the OLD cluster:
	client_old, err := NewClient(ToUint128(0), []string{"3000"})
	if err != nil {
		log.Fatalf("Error creating client old: %s", err)
	}
	defer client_old.Close()

	// Set here the address of the NEW cluster:
	client_new, err := NewClient(ToUint128(0), []string{"3001"})
	if err != nil {
		log.Fatalf("Error creating client new: %s", err)
	}
	defer client_old.Close()

	expected := make([]Account, 0)
	account_timestamp_previous := uint(0)

	fmt.Println("Importing accounts ...")
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
		expected = append(expected, accounts...)

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

		account_timestamp_previous = uint(accounts[len(accounts)-1].Timestamp) + 1
	}
	if len(expected) == 0 {
		log.Fatal("Nothing to export")
	}

	fmt.Println("Importing transfers...")
	transfer_timestamp_previous := uint(0)
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

		transfer_timestamp_previous = uint(transfers[len(transfers)-1].Timestamp) + 1
	}

	fmt.Println("Validating balances...")
	for {
		accounts, err := client_new.QueryAccounts(QueryFilter{
			TimestampMin: expected[0].Timestamp,
			Limit:        8190,
		})
		if err != nil {
			log.Fatalf("Error loading accounts: %s", err)
		}

		for i := 0; i < len(accounts); i++ {
			// Asserting the balance before and after:
			equals :=
				accounts[i].Timestamp == expected[i].Timestamp &&
					reflect.DeepEqual(accounts[i].DebitsPending.Bytes(), expected[i].DebitsPending.Bytes()) &&
					reflect.DeepEqual(accounts[i].DebitsPosted.Bytes(), expected[i].DebitsPosted.Bytes()) &&
					reflect.DeepEqual(accounts[i].CreditsPending.Bytes(), expected[i].CreditsPending.Bytes()) &&
					reflect.DeepEqual(accounts[i].CreditsPosted.Bytes(), expected[i].CreditsPosted.Bytes())
			if !equals {
				log.Fatalf("Error accounts differ: %d -> %d", accounts[i].Timestamp, expected[i].Timestamp)
			}
		}

		if len(expected) == len(accounts) {
			break
		}
		expected = expected[len(accounts):]
	}

	fmt.Println("Finished")
}
