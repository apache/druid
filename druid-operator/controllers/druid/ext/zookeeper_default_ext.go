package ext

type DefaultZkManager struct {
	Properties string `json:"properties"`
}

func (p *DefaultZkManager) Configuration() string {
	return p.Properties
}
