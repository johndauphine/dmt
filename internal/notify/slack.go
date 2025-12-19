package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/johndauphine/mssql-pg-migrate/internal/config"
)

// Notifier sends notifications to Slack
type Notifier struct {
	config     *config.SlackConfig
	httpClient *http.Client
}

// SlackMessage represents a Slack webhook message
type SlackMessage struct {
	Channel     string            `json:"channel,omitempty"`
	Username    string            `json:"username,omitempty"`
	IconEmoji   string            `json:"icon_emoji,omitempty"`
	Text        string            `json:"text,omitempty"`
	Attachments []SlackAttachment `json:"attachments,omitempty"`
}

// SlackAttachment represents a Slack message attachment
type SlackAttachment struct {
	Color      string       `json:"color,omitempty"`
	Title      string       `json:"title,omitempty"`
	Text       string       `json:"text,omitempty"`
	Fields     []SlackField `json:"fields,omitempty"`
	Footer     string       `json:"footer,omitempty"`
	FooterIcon string       `json:"footer_icon,omitempty"`
	Timestamp  int64        `json:"ts,omitempty"`
}

// SlackField represents a field in a Slack attachment
type SlackField struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

// New creates a new Slack notifier
func New(cfg *config.SlackConfig) *Notifier {
	if cfg == nil {
		cfg = &config.SlackConfig{Enabled: false}
	}
	return &Notifier{
		config: cfg,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// IsEnabled returns true if notifications are enabled
func (n *Notifier) IsEnabled() bool {
	return n.config != nil && n.config.Enabled && n.config.WebhookURL != ""
}

// MigrationStarted sends notification when migration starts
func (n *Notifier) MigrationStarted(runID string, sourceDB, targetDB string, tableCount int) error {
	if !n.IsEnabled() {
		return nil
	}

	msg := SlackMessage{
		Channel:   n.config.Channel,
		Username:  n.getUsername(),
		IconEmoji: ":rocket:",
		Attachments: []SlackAttachment{
			{
				Color: "#36a64f", // green
				Title: "Migration Started",
				Fields: []SlackField{
					{Title: "Run ID", Value: runID, Short: true},
					{Title: "Tables", Value: fmt.Sprintf("%d", tableCount), Short: true},
					{Title: "Source", Value: sourceDB, Short: true},
					{Title: "Target", Value: targetDB, Short: true},
				},
				Footer:    "mssql-pg-migrate",
				Timestamp: time.Now().Unix(),
			},
		},
	}

	return n.send(msg)
}

// MigrationCompleted sends notification when migration completes successfully
func (n *Notifier) MigrationCompleted(runID string, startTime time.Time, duration time.Duration, tableCount int, rowCount int64, throughput float64) error {
	if !n.IsEnabled() {
		return nil
	}

	headerText := fmt.Sprintf("Migration pipeline completed successfully. Migrated %d tables with %s total rows. Throughput: %s rows/sec.",
		tableCount, formatNumberWithCommas(rowCount), formatNumberWithCommas(int64(throughput)))

	msg := SlackMessage{
		Channel:   n.config.Channel,
		Username:  n.getUsername(),
		IconEmoji: ":white_check_mark:",
		Text:      headerText,
		Attachments: []SlackAttachment{
			{
				Color: "#36a64f", // green
				Fields: []SlackField{
					{Title: "Run ID", Value: runID, Short: true},
					{Title: "Started", Value: startTime.UTC().Format("2006-01-02 15:04:05 UTC"), Short: true},
					{Title: "Duration", Value: formatDuration(duration), Short: true},
					{Title: "Tables", Value: fmt.Sprintf("%d", tableCount), Short: true},
					{Title: "Total Rows", Value: formatNumberWithCommas(rowCount), Short: true},
					{Title: "Throughput", Value: fmt.Sprintf("%s rows/sec", formatNumberWithCommas(int64(throughput))), Short: true},
				},
				Footer:    "mssql-pg-migrate",
				Timestamp: time.Now().Unix(),
			},
		},
	}

	return n.send(msg)
}

// MigrationFailed sends notification when migration fails
func (n *Notifier) MigrationFailed(runID string, err error, duration time.Duration) error {
	if !n.IsEnabled() {
		return nil
	}

	errMsg := "Unknown error"
	if err != nil {
		errMsg = err.Error()
		if len(errMsg) > 500 {
			errMsg = errMsg[:500] + "..."
		}
	}

	msg := SlackMessage{
		Channel:   n.config.Channel,
		Username:  n.getUsername(),
		IconEmoji: ":x:",
		Attachments: []SlackAttachment{
			{
				Color: "#dc3545", // red
				Title: "Migration Failed",
				Fields: []SlackField{
					{Title: "Run ID", Value: runID, Short: true},
					{Title: "Duration", Value: duration.Round(time.Second).String(), Short: true},
					{Title: "Error", Value: errMsg, Short: false},
				},
				Footer:    "mssql-pg-migrate",
				Timestamp: time.Now().Unix(),
			},
		},
	}

	return n.send(msg)
}

// TableTransferFailed sends notification for individual table failures
func (n *Notifier) TableTransferFailed(runID, tableName string, err error) error {
	if !n.IsEnabled() {
		return nil
	}

	errMsg := "Unknown error"
	if err != nil {
		errMsg = err.Error()
	}

	msg := SlackMessage{
		Channel:   n.config.Channel,
		Username:  n.getUsername(),
		IconEmoji: ":warning:",
		Attachments: []SlackAttachment{
			{
				Color: "#ffc107", // yellow
				Title: "Table Transfer Failed",
				Fields: []SlackField{
					{Title: "Run ID", Value: runID, Short: true},
					{Title: "Table", Value: tableName, Short: true},
					{Title: "Error", Value: errMsg, Short: false},
				},
				Footer:    "mssql-pg-migrate",
				Timestamp: time.Now().Unix(),
			},
		},
	}

	return n.send(msg)
}

func (n *Notifier) send(msg SlackMessage) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}

	resp, err := n.httpClient.Post(n.config.WebhookURL, "application/json", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("sending to Slack: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Slack returned status %d", resp.StatusCode)
	}

	return nil
}

func (n *Notifier) getUsername() string {
	if n.config.Username != "" {
		return n.config.Username
	}
	return "mssql-pg-migrate"
}

func formatNumberWithCommas(n int64) string {
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}

	var result []byte
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
