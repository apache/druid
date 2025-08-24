package ext

type DefaultMetadataStoreManager struct {
	Properties string `json:"properties"`
}

func (p *DefaultMetadataStoreManager) Configuration() string {
	return p.Properties
}
