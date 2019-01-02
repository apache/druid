import * as React from "react"
import * as d3 from 'd3'

export class Axis extends React.Component {

    componentDidMount() {
        this.renderAxis()
    }

    componentDidUpdate() {
        this.renderAxis()
    }

    renderAxis() {
        const axis = d3.axisBottom(this.props.xScale)

        d3.select(this.refs.g)
            .call(axis)
    }

    render() {
        return <g transform="translate(0, 30)" ref="g"/>
    }
}
