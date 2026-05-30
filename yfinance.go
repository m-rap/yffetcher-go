package yffetcher

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/m-rap/decimal-go"
	itp "github.com/m-rap/itemprice-go"
)

type priceResult struct {
	itemID     string
	datetimeMs int64
	high       float64
	low        float64
	open       float64
	close      float64
	err        error
}

type YahooResponse struct {
	Chart struct {
		Result []struct {
			Indicators struct {
				Quote []struct {
					Open  []float64 `json:"open"`
					High  []float64 `json:"high"`
					Low   []float64 `json:"low"`
					Close []float64 `json:"close"`
				} `json:"quote"`
			} `json:"indicators"`
		} `json:"result"`
	} `json:"chart"`
}

var ErrMarketClosed = errors.New("market closed")

func fetchStockDailyAsync(wg *sync.WaitGroup, itemID string, date time.Time, results chan<- priceResult) {
	defer wg.Done()

	ticker := itemID + ".jk"

	// Use interval=1d to get daily OHLC for the range

	// dateStr := date.Format("2006-01-02")
	p1 := date.Unix()
	// p2 := p1 + 86400 // 24 hours later
	p2 := p1 + 12*60*60 // 12 hour later

	params := url.Values{}
	params.Add("period1", fmt.Sprintf("%d", p1))
	params.Add("period2", fmt.Sprintf("%d", p2))
	params.Add("interval", "1d")

	url := fmt.Sprintf("https://query1.finance.yahoo.com/v8/finance/chart/%s?%s",
		url.PathEscape(ticker), params.Encode())

	// fmt.Printf("requesting url %s\n", url)

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		results <- priceResult{itemID: itemID, datetimeMs: date.UnixMilli(), err: err}
		return
	}
	defer resp.Body.Close()
	// buff, err := io.ReadAll(resp.Body)
	// if err != nil {
	// 	fmt.Printf("err read body content: %v\n", err)
	// } else {
	// 	fmt.Printf("body: %s\n", string(buff))
	// }

	var data YahooResponse
	// if err := json.NewDecoder(bytes.NewReader(buff)).Decode(&data); err != nil {
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		results <- priceResult{itemID: itemID, datetimeMs: date.UnixMilli(), err: err}
		return
	}

	if len(data.Chart.Result) > 0 {
		q := data.Chart.Result[0].Indicators.Quote[0]
		// Yahoo sometimes returns empty slices for weekends/holidays
		if len(q.Close) > 0 && q.Close[0] != 0 {
			results <- priceResult{
				itemID:     itemID,
				datetimeMs: date.UnixMilli(),
				high:       q.High[0],
				low:        q.Low[0],
				open:       q.Open[0],
				close:      q.Close[0],
			}
			return
		}
	}
	results <- priceResult{itemID: itemID, datetimeMs: date.UnixMilli(), err: err}
}

// add .JK suffix for idx stock
// Example Range: May 1st to May 15th, 2024
// start, _ := time.Parse("2006-01-02", "2024-05-01")
// end, _ := time.Parse("2006-01-02", "2024-05-15")
func fetchStocksData(items []*itp.Item, start time.Time, end time.Time) map[string][]*priceResult {
	resultsChan := make(chan priceResult, len(items))
	var wg sync.WaitGroup
	activeCount := 0
	batchSize := 100

	stockRes := make(map[string][]*priceResult)

	// 1. Start the receiver in the background
	go func() {
		for res := range resultsChan {
			arr, ok := stockRes[res.itemID]
			if !ok {
				arr = []*priceResult{}
			}
			arr = append(arr, &res)
			if len(arr)%50 == 0 {
				fmt.Printf("ticker %s collected %d data\n", res.itemID, len(arr))
			}
			stockRes[res.itemID] = arr
		}
	}()

	// Iterate through tickers and dates
	for _, item := range items {
		fmt.Printf("fetching %s...\n", item.ID)
		for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
			// Skip weekends locally to save requests
			if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
				continue
			}
			wg.Add(1)
			activeCount++
			go fetchStockDailyAsync(&wg, item.ID, d, resultsChan)

			// Check if we reached the batch limit
			if activeCount == batchSize {
				wg.Wait()       // Wait for these 5 to finish
				activeCount = 0 // Reset independent counter

				//fmt.Println("Batch complete, resting...")
				//time.Sleep(1 * time.Second)
				time.Sleep(20 * time.Millisecond)
			}
		}
	}

	wg.Wait()          // Final wait for remainder
	close(resultsChan) // Closing the channel stops the receiver loop

	for k, arr := range stockRes {
		fmt.Printf("ticker %s finished collected %d data. sorting data...\n", k, len(arr))
		sort.Slice(arr, func(i, j int) bool {
			return arr[i].datetimeMs < arr[j].datetimeMs
		})
		for _, priceRes := range arr {
			if priceRes.err != nil && priceRes.err != ErrMarketClosed {
				d := time.UnixMilli(priceRes.datetimeMs)
				fmt.Printf("stock %s %s %.0f %.0f %.0f err %v\n", k, d.Format("01-02-2006"), priceRes.high, priceRes.low, priceRes.close, priceRes.err)
			}
		}
	}
	fmt.Println("end of fetch data.")
	return stockRes
}

func FetchFromJson(itemPriceDbFileName string, itemFilterJson []byte) error {
	itemFilter, err := itp.ItemJsonToArr(itemFilterJson)
	if err != nil {
		return fmt.Errorf("error parsing item json: %v", err)
	}

	return Fetch(itemPriceDbFileName, itemFilter)
}

func Fetch(itemPriceDbFileName string, itemFilter []itp.Item) error {
	db, err := itp.OpenOrCreateDB(itemPriceDbFileName)
	if err != nil {
		return fmt.Errorf("open or create db error: %v", err)
	}
	defer db.Close()

	nowUtc := time.Now().UTC()
	var tStart time.Time
	tStartMsMin := int64(math.MaxInt64)
	items := []*itp.Item{}
	itemMap := make(map[string]*itp.Item)
	for i := range itemFilter {

		item, err := itp.GetItemByName(db, itemFilter[i].Name)
		if err == sql.ErrNoRows {
			item = itemFilter[i].Copy()
			item.Unit = "share"
			insertErr := itp.InsertItem(db, item)
			if insertErr != nil {
				return fmt.Errorf("insert item error: %v", insertErr)
			}
			fmt.Printf("item inserted to DB\n")
		} else if err != nil {
			return fmt.Errorf("get item by name error: %v", err)
		} else {
			fmt.Printf("item already exists in DB %v.\n", item.ID)

		}
		items = append(items, item)
		itemMap[item.ID] = item

		period, err := itp.ChooseFetchPeriod(db, item)
		if err != nil {
			return fmt.Errorf("choose fetch period error: %v", err)
		}

		//nowUtcMs := nowUtc.UnixMilli()
		var tmpStart time.Time
		switch period {
		case itp.Period1y:
			tmpStart = nowUtc.AddDate(-1, 0, 0)
		case itp.Period6m:
			tmpStart = nowUtc.AddDate(0, -6, 0)
		case itp.Period3m:
			tmpStart = nowUtc.AddDate(0, -3, 0)
		case itp.Period1m:
			tmpStart = nowUtc.AddDate(0, -1, 0)
		}
		if tmpStart.UnixMilli() < tStartMsMin {
			tStart = tmpStart
			tStartMsMin = tmpStart.UnixMilli()
		}
	}

	itpResultMap := fetchStocksData(items, tStart, nowUtc)

	itemPrices := []*itp.ItemPrice{}
	for _, itpResults := range itpResultMap {
		for _, itpRes := range itpResults {
			if itpRes.err != nil {
				if itpRes.err != ErrMarketClosed {
					d := time.UnixMilli(itpRes.datetimeMs)
					fmt.Printf("err fetching %s %s: %v\n", itpRes.itemID, d.Format("06-0102"), itpRes.err)
				}
				continue
			}
			itemPrices = append(itemPrices, &itp.ItemPrice{
				Item:       itemMap[itpRes.itemID],
				DatetimeMs: itpRes.datetimeMs,
				OpenPrice:  decimal.NewDecimalFromFloat(itpRes.open),
				ClosePrice: decimal.NewDecimalFromFloat(itpRes.close),
				HighPrice:  decimal.NewDecimalFromFloat(itpRes.high),
				LowPrice:   decimal.NewDecimalFromFloat(itpRes.low),
			})
		}
	}

	if len(itemPrices) == 0 {
		return fmt.Errorf("empty result")
	}
	err = itp.InsertItemPrices(db, itemPrices)
	if err != nil {
		return fmt.Errorf("insert item prices error: %v", err)
	}
	fmt.Printf("insert %d item prices successfull.\n", len(itemPrices))

	return nil
}
