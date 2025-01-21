package srv

type (
	Config struct {
		Strategy uint8    `json:"strategy,omitempty"`
		Peers    []string `json:"peers,omitempty"`
	}
)
