package yffetcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	itp "github.com/m-rap/itemprice-go"
)

type ItemPriceResult struct {
	ItemPrice *itp.ItemPrice
	Err       error
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

func FetchStockDailyAsync(wg *sync.WaitGroup, item *itp.Item, date time.Time, results chan<- *ItemPriceResult) {
	defer wg.Done()

	ticker := item.ID + ".jk"

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
		results <- &ItemPriceResult{ItemPrice: &itp.ItemPrice{Item: item, DatetimeMs: date.UnixMilli()}, Err: err}
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
		results <- &ItemPriceResult{ItemPrice: &itp.ItemPrice{Item: item, DatetimeMs: date.UnixMilli()}, Err: err}
		return
	}

	if len(data.Chart.Result) > 0 {
		q := data.Chart.Result[0].Indicators.Quote[0]
		// Yahoo sometimes returns empty slices for weekends/holidays
		if len(q.Close) > 0 && q.Close[0] != 0 {
			results <- ItemPriceResult{
				ItemPrice: &itm.ItemPrice{
					Ticker:     ticker,
					Date:       date,
					HighPrice:  q.High[0],
					LowPrice:   q.Low[0],
					OpenPrice:  q.Open[0],
					ClosePrice: q.Close[0],
				},
			}
			return
		}
	}
	results <- &ItemPriceResult{ItemPrice: &itp.ItemPrice{Item: item, DatetimeMs: date.UnixMilli()}, Err: ErrMarketClosed}
}

// add .JK suffix for idx stock
// Example Range: May 1st to May 15th, 2024
// start, _ := time.Parse("2006-01-02", "2024-05-01")
// end, _ := time.Parse("2006-01-02", "2024-05-15")
func FetchStocksData(items []*itp.Item, start time.Time, end time.Time) map[string][]*ItemPriceResult {
	resultsChan := make(chan *ItemPriceResult, len(items))
	var wg sync.WaitGroup
	activeCount := 0
	batchSize := 100

	stockRes := make(map[string][]*ItemPriceResult)

	// 1. Start the receiver in the background
	go func() {
		for res := range resultsChan {
			arr, ok := stockRes[res.ItemPrice.Item.ID]
			if !ok {
				arr = []*ItemPriceResult{}
			}
			arr = append(arr, &res)
			if len(arr)%50 == 0 {
				fmt.Printf("ticker %s collected %d data\n", res.Ticker, len(arr))
			}
			stockRes[res.Ticker] = arr
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
			go FetchStockDailyAsync(&wg, item, d, resultsChan)

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
			return arr[i].ItemPrice.DatetimeMs < arr[j].ItemPrice.DatetimeMs
		})
		for _, daily := range arr {
			if daily.Err != nil && daily.Err != ErrMarketClosed {
				d := time.UnixMilli(daily.ItemPrice.DatetimeMs)
				fmt.Printf("stock %s %s %.0f %.0f %.0f err %v\n", k, d.Format("01-02-2006"), daily.ItemPrice.HighPrice.ToFloat(), daily.ItemPrice.LowPrice.ToFloat(), daily.ItemPrice.ClosePrice.ToFloat(), daily.Err)
			}
		}
	}
	fmt.Println("end of fetch data.")
	return stockRes
}
