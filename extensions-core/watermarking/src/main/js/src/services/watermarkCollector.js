import 'whatwg-fetch'

export class WatermarkCollectorService {

    static fetchStatus() {
        console.log('fetching loadstatus')
        return fetch(`/proxy/druid/watermarking/v1/keeper`, {
            redirect: 'follow'
        }).then((response) => {
            return response.json()
        }).then((json) => {
            console.log('parsed json', json)
            return json
        })
    }

    static updateWatermark(datasource, metadataType, timestamp) {
        const path = `${datasource}/${metadataType}?timestamp=${timestamp}`
        return fetch(`/proxy/druid/watermarking/v1/keeper/datasources/${path}`, {
            method: 'POST',
            redirect: 'follow'
        }).then((response) => {
            console.log(response)
            return true
        })
    }
}
