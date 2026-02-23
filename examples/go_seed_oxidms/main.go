// Seed program for OxiDMS: creates a form with all field types and inserts 1000 submissions.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

const baseURL = "https://oxidms.baltavista.com/api/v1"

var client = &http.Client{Timeout: 30 * time.Second}

// ── helpers ──────────────────────────────────────────────────────────────────

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func login(email, password string) string {
	body := must(json.Marshal(map[string]string{"email": email, "password": password}))
	resp := must(http.Post(baseURL+"/auth/login", "application/json", bytes.NewReader(body)))
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw := must(io.ReadAll(resp.Body))
		panic(fmt.Sprintf("login failed (%d): %s", resp.StatusCode, raw))
	}
	var result struct{ Token string }
	json.NewDecoder(resp.Body).Decode(&result)
	return result.Token
}

func apiPost(token, path string, payload any) map[string]any {
	body := must(json.Marshal(payload))
	req := must(http.NewRequest("POST", baseURL+path, bytes.NewReader(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp := must(client.Do(req))
	defer resp.Body.Close()
	raw := must(io.ReadAll(resp.Body))
	if resp.StatusCode >= 400 {
		panic(fmt.Sprintf("POST %s (%d): %s", path, resp.StatusCode, raw))
	}
	var result map[string]any
	json.Unmarshal(raw, &result)
	return result
}

// ── random data generators ───────────────────────────────────────────────────

var (
	firstNames = []string{
		"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank",
		"Iris", "Jack", "Kara", "Leo", "Mona", "Noah", "Olivia", "Paul",
		"Quinn", "Ruby", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
		"Yara", "Zane", "Amara", "Boris", "Clara", "Derek",
	}
	lastNames = []string{
		"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
		"Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
		"Hernandez", "Moore", "Martin", "Jackson", "Thompson", "White",
		"Lopez", "Lee", "Harris", "Clark", "Lewis", "Robinson",
	}
	cities = []string{
		"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
		"San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
		"Fort Worth", "Columbus", "Charlotte", "Indianapolis", "Seattle", "Denver",
		"Boston", "Nashville", "Portland", "Las Vegas", "Memphis", "Louisville",
	}
	departments = []string{"Engineering", "Marketing", "Sales", "HR", "Finance", "Legal", "Operations", "Support"}
	priorities  = []string{"Low", "Medium", "High", "Critical"}
	statuses    = []string{"New", "In Progress", "Under Review", "Completed"}
	categories  = []string{"Bug Report", "Feature Request", "Improvement", "Documentation", "Security"}
	tags        = []string{
		"urgent", "backend", "frontend", "database", "performance", "ux",
		"security", "devops", "testing", "mobile", "api", "infra",
	}
	loremWords = strings.Fields(
		"lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor " +
			"incididunt ut labore et dolore magna aliqua ut enim ad minim veniam quis " +
			"nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat " +
			"duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore " +
			"eu fugiat nulla pariatur excepteur sint occaecat cupidatat non proident sunt " +
			"in culpa qui officia deserunt mollit anim id est laborum")
)

func pick[T any](s []T) T                   { return s[rand.Intn(len(s))] }
func pickN[T any](s []T, n int) []T         { out := make([]T, n); for i := range out { out[i] = pick(s) }; return out }
func randEmail(first, last string) string    { return fmt.Sprintf("%s.%s%d@example.com", strings.ToLower(first), strings.ToLower(last), rand.Intn(100)) }
func randDate(from, to time.Time) string     { d := to.Sub(from); return from.Add(time.Duration(rand.Int63n(int64(d)))).Format("2006-01-02") }
func randPhone() string                      { return fmt.Sprintf("+1-%03d-%03d-%04d", rand.Intn(900)+100, rand.Intn(900)+100, rand.Intn(10000)) }
func randLorem(minW, maxW int) string {
	n := minW + rand.Intn(maxW-minW+1)
	words := make([]string, n)
	for i := range words {
		words[i] = loremWords[rand.Intn(len(loremWords))]
	}
	s := strings.Join(words, " ")
	return strings.ToUpper(s[:1]) + s[1:] + "."
}

func randSubmission(i int) map[string]any {
	first := pick(firstNames)
	last := pick(lastNames)
	return map[string]any{
		// text
		"full_name": first + " " + last,
		// textarea
		"description": randLorem(15, 50),
		// number
		"age": rand.Intn(50) + 18,
		// email
		"contact_email": randEmail(first, last),
		// date
		"start_date": randDate(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC)),
		// select
		"department": pick(departments),
		// checkbox
		"is_active": rand.Intn(2) == 1,
		// radio
		"priority": pick(priorities),
		// additional fields for richer data
		"city":       pick(cities),
		"phone":      randPhone(),
		"salary":     float64(rand.Intn(150000)+30000) + float64(rand.Intn(100))/100,
		"notes":      randLorem(20, 80),
		"status":     pick(statuses),
		"category":   pick(categories),
		"score":      float64(rand.Intn(100)) + float64(rand.Intn(100))/100,
		"join_date":  randDate(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		"tags":       strings.Join(pickN(tags, 2+rand.Intn(3)), ", "),
		"approved":   rand.Intn(3) != 0,
		"experience": pick([]string{"Junior", "Mid", "Senior", "Lead"}),
	}
}

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	if len(os.Args) > 1 && os.Args[1] == "bulk" {
		bulkMain()
		return
	}

	fmt.Println("🔑 Logging in...")
	token := login("admin@oxidms.local", "admin123")
	fmt.Println("   ✓ Authenticated")

	// Define form with all supported field types
	fields := []map[string]any{
		{"name": "full_name", "label": "Full Name", "type": "text", "required": true, "placeholder": "Enter full name", "indexed": true, "x": 20, "y": 20, "w": 300, "h": 80},
		{"name": "description", "label": "Description", "type": "textarea", "required": false, "placeholder": "Describe...", "x": 20, "y": 120, "w": 300, "h": 120},
		{"name": "age", "label": "Age", "type": "number", "required": true, "placeholder": "0", "min": 0, "max": 120, "indexed": true, "x": 340, "y": 20, "w": 300, "h": 80},
		{"name": "contact_email", "label": "Contact Email", "type": "email", "required": true, "placeholder": "email@example.com", "x": 340, "y": 120, "w": 300, "h": 80},
		{"name": "start_date", "label": "Start Date", "type": "date", "required": true, "indexed": true, "x": 20, "y": 260, "w": 300, "h": 80},
		{"name": "department", "label": "Department", "type": "select", "required": true, "options": departments, "indexed": true, "x": 340, "y": 220, "w": 300, "h": 80},
		{"name": "is_active", "label": "Is Active", "type": "checkbox", "required": false, "x": 20, "y": 360, "w": 200, "h": 60},
		{"name": "priority", "label": "Priority", "type": "radio", "required": true, "options": priorities, "x": 240, "y": 360, "w": 200, "h": 80},
		{"name": "attachment", "label": "Attachment", "type": "file", "required": false, "x": 460, "y": 360, "w": 300, "h": 80},
		// extra fields for comprehensive data
		{"name": "city", "label": "City", "type": "text", "required": false, "placeholder": "City name", "indexed": true, "x": 20, "y": 460, "w": 300, "h": 80},
		{"name": "phone", "label": "Phone", "type": "text", "required": false, "placeholder": "+1-xxx-xxx-xxxx", "x": 340, "y": 460, "w": 300, "h": 80},
		{"name": "salary", "label": "Salary", "type": "number", "required": false, "placeholder": "0.00", "x": 20, "y": 560, "w": 300, "h": 80},
		{"name": "notes", "label": "Notes", "type": "textarea", "required": false, "placeholder": "Additional notes...", "x": 340, "y": 560, "w": 300, "h": 120},
		{"name": "status", "label": "Status", "type": "select", "required": false, "options": statuses, "indexed": true, "x": 20, "y": 660, "w": 300, "h": 80},
		{"name": "category", "label": "Category", "type": "radio", "required": false, "options": categories, "x": 340, "y": 700, "w": 300, "h": 100},
		{"name": "score", "label": "Score", "type": "number", "required": false, "placeholder": "0-100", "min": 0, "max": 100, "x": 20, "y": 760, "w": 300, "h": 80},
		{"name": "join_date", "label": "Join Date", "type": "date", "required": false, "x": 340, "y": 820, "w": 300, "h": 80},
		{"name": "tags", "label": "Tags", "type": "text", "required": false, "placeholder": "comma-separated tags", "x": 20, "y": 860, "w": 300, "h": 80},
		{"name": "approved", "label": "Approved", "type": "checkbox", "required": false, "x": 340, "y": 920, "w": 200, "h": 60},
		{"name": "experience", "label": "Experience Level", "type": "select", "required": false, "options": []string{"Junior", "Mid", "Senior", "Lead"}, "x": 20, "y": 960, "w": 300, "h": 80},
	}

	fmt.Println("📋 Creating form...")
	formResult := apiPost(token, "/forms", map[string]any{
		"name":        "Employee Survey — All Field Types",
		"description": "Comprehensive form with every supported field type (text, textarea, number, email, date, select, checkbox, radio, file) plus indexed fields for search.",
		"fields":      fields,
	})
	formID, _ := formResult["_id"].(string)
	fmt.Printf("   ✓ Form created: id=%s slug=%s\n", formID, formResult["slug"])

	// Insert 1000 submissions
	fmt.Println("📝 Inserting 1000 submissions...")
	start := time.Now()
	batchSize := 50
	for batch := 0; batch < 1000/batchSize; batch++ {
		for i := 0; i < batchSize; i++ {
			idx := batch*batchSize + i
			data := randSubmission(idx)
			apiPost(token, "/forms/"+formID+"/submissions", map[string]any{"data": data})
		}
		done := (batch + 1) * batchSize
		elapsed := time.Since(start)
		rate := float64(done) / elapsed.Seconds()
		fmt.Printf("   %4d/1000  (%.0f submissions/sec)\n", done, rate)
	}

	elapsed := time.Since(start)
	fmt.Printf("\n✅ Done! Inserted 1000 submissions in %s (%.0f/sec)\n", elapsed.Round(time.Millisecond), 1000.0/elapsed.Seconds())
	fmt.Printf("   Form: %s/forms/%s\n", "https://oxidms.baltavista.com", formID)
}
