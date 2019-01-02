
import 'whatwg-fetch'
import React from 'react'
import { render } from 'react-dom'
import { Button, Container, Header, Menu, Segment, Dropdown } from 'semantic-ui-react'
import { WatermarkCollectorStatus } from './components/watermarkCollectorStatus'

const MOUNT_NODE = document.getElementById('root')



const App = () => (
  <Container>
    <WatermarkCollectorStatus/>
  </Container>
)

render(<App />, MOUNT_NODE)
