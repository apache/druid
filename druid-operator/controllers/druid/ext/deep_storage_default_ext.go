package ext

type DefaultDeepStorageManager struct {
	Properties string `json:"properties"`
}

func (p *DefaultDeepStorageManager) Configuration() string {
	return p.Properties
}
