
import 'whatwg-fetch'
import React from 'react'
import * as _ from 'lodash'
import sizeMe from 'react-sizeme'
import AutoScale from 'react-auto-scale';
import * as moment from 'moment'
import { Icon, Step, Modal, Header, Button, Input,Segment, Container, Menu, Dropdown } from 'semantic-ui-react'

import { WatermarkCollectorService } from '../services/watermarkCollector'
import { TimelineChart } from './timelineChart'

const options = [{ text: 'watermarks', value: 'watermarks' }]

export class WatermarkCollectorStatus extends React.Component {

  constructor(props) {
    super(props)
    this.state = {
      inventoryInitialized: <Icon name='circle notched' loading />,
      timelines: [],
      openModal: false,
      updateDatasource: '',
      updateType: '',
      updateTimestamp: '',
      env: 'watermarks'
    }

    this.fetchLoadStatus()
  }

  openModal(dataSource, metadataType, timestamp) {
    this.setState({ openModal: true, updateDatasource: dataSource, updateType: metadataType, updateTimestamp: timestamp })
  }

  closeModal = () => {
    this.setState({ openModal: false, updateDatasource: '', updateType: '', updateTimestamp:'' })
  }

  fetchLoadStatus() {
    return WatermarkCollectorService.fetchStatus().then((watermarkStatus) => {
      this.setState({
          inventoryInitialized: 'Initialized',
          timelines: watermarkStatus
      })
    }).catch((ex) => {
        console.log('parsing failed', ex)
    })
  }

  updateWatermark() {
    console.log('update:' + this.state.updateDatasource +
        ":" + this.state.updateType + " to " +
        this.state.updateTimestamp)
    WatermarkCollectorService.updateWatermark(
        this.state.updateDatasource,
        this.state.updateType,
        this.state.updateTimestamp
    ).then(() => {
      return this.fetchLoadStatus()
    }).catch((ex) => {
      console.log('parsing failed', ex)
    })
  }


  render() {
    const handleChange = (event) => {
      this.setState({updateTimestamp: event.target.value})
    }

    const getStep = (dataSource, metadataKey, timestamp) => {
      return (
        <Step link onClick={() => this.openModal(dataSource, metadataKey, timestamp.replace("Z", ":00.000Z"))}>
          <Icon name='time' />
          <Step.Content>
            <Step.Title>{metadataKey}</Step.Title>
            <Step.Description><small>{timestamp}</small></Step.Description>
          </Step.Content>
        </Step> 
      )
    }

    const getDisabledStep = (metadataKey, timestamp) => {
      return (
        <Step>
          <Icon name='time' disabled/>
          <Step.Content>
            <Step.Title>{metadataKey}</Step.Title>
            <Step.Description><small>{timestamp}</small></Step.Description>
          </Step.Content>
        </Step> 
      )
    }
    
    const rows = _.orderBy(Object.keys(this.state.timelines)).map((dataSource, index) => {
      const preferredOrder = ['mintime', 'batch_low', 'batch_high', 'stable_low', 'stable_high', 'maxtime']
      const immutable = ['mintime', 'maxtime','batch_high', 'stable_high']
      const raw = Object.keys(this.state.timelines[dataSource]).map((key) => { 
        return { type: key, timestamp: new Date(this.state.timelines[dataSource][key]), label: this.state.timelines[dataSource][key].replace(':00.000',''), value: 1, preferred: preferredOrder.indexOf(key)}
      })
      const order = _.orderBy(raw, ['timestamp', 'preferred'],['asc', 'asc'])
      const steps = order.map((entry) => {
        return immutable.indexOf(entry.type) > -1 ? getDisabledStep(entry.type, entry.label) : getStep(dataSource, entry.type, entry.label)
      })

        const chartForm = order.map((entry) => {
          return { x: entry.timestamp, y: 5 }
        })
        const data = chartForm

        let icon = 'check circle'
        let colorStatus = 'green'
        let errMessage = null
        let oneweek = 1000 * 60 * 60 * 24 * 7
        let hasGaps =
            (
                new Date(this.state.timelines[dataSource]['stable_high']).getTime()
                !== new Date(this.state.timelines[dataSource]['stable_low']).getTime()
            ) ||
            (
                new Date(this.state.timelines[dataSource]['batch_high']).getTime()
                !== new Date(this.state.timelines[dataSource]['batch_low']).getTime()
            );

        if (hasGaps) {
          icon = 'alarm'
          colorStatus = 'red'
        }
      return (
        <Segment secondary>
          <h3><Icon name={icon} color={colorStatus}/> {dataSource}</h3>
          <Container textAlign="center">
            <TimelineChart data={data}/>
            <Step.Group size='mini'>
              {steps}
            </Step.Group>
          </Container>
        </Segment>
      )
    })


    return (
        <Container>
          <Menu pointing secondary inverted>
            <Menu.Item header>
              <img src="img/druid_nav.png" style={{width: 105 + 'px', paddingRight: 10 + 'px'}} />
              Datasource Watermark Levels
            </Menu.Item>
          </Menu>
          <Segment inverted secondary>
            <div>
              <h3>{this.state.inventoryInitialized}</h3>
                {rows}
              <Modal open={this.state.openModal} basic size='small' onClose={this.closeModal}>
                <Header icon='archive'>Update {this.state.updateType} watermark</Header>
                <Modal.Content>
                  <Container>
                    <p>
                      <b>⚠<i>WARNING</i>⚠</b> Updating this will require you to <b><i>restart</i></b> the collector for <b><i>{this.state.updateDatasource}</i></b>
                    </p>
                    <p>
                      Low watermarks can be artificially raised in cases where there are data gaps which should be
                      ignored or are causing any sort of issues.
                    </p>
                    <p>
                      Would you like to update the <strong>{this.state.updateType}</strong> watermark for {this.state.updateDatasource}?
                    </p>
                    <Input fluid value={this.state.updateTimestamp} onChange={handleChange}/>
                  </Container>
                </Modal.Content>
                <Modal.Actions>
                  <Button basic color='red' inverted onClick={this.closeModal}>
                    <Icon name='remove' /> Cancel
                  </Button>
                  <Button color='green' inverted onClick={
                      () => {
                          this.updateWatermark()
                          this.closeModal()
                      }
                  }>
                    <Icon name='checkmark' /> Update
                  </Button>
                </Modal.Actions>
              </Modal>
            </div>
          </Segment>
        </Container>
    )
  }
}
