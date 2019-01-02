import * as React from "react"
import * as d3 from "d3"
import { Chart } from './chart'
import { Timeline } from './timeline'
import { Axis } from './axis'

export class TimelineChart extends React.Component {
    static defaultProps = {
        width: 640,
        height: 60
    }

    render() {
        const padding = 10
        const { data, width, height, color } = this.props
        const size = {
            width: width - padding*2,
            height: height - padding*2
        }

        let min = new Date(2015, 0, 1)
        let max = new Date()

        if (data && data.length > 0) {
            min = new Date(data[0].x)
            max =new Date(data[data.length-1].x)
        }

        const xScale = d3.scaleTime()
            .domain([min, max])
            .range([padding, width-(padding*2)])

        const yScale = d3.scaleLinear()
            .domain([0, 10])
            .range([height-padding, padding])

        return (
            <Chart width={width} height={height} padding={padding}>
                <Axis
                    xScale={xScale} ref="axis"
                />
                <Timeline
                    data={data}
                    size={size}
                    xScale={xScale}
                    yScale={yScale}
                    ref="data"
                    color={color}
                />
            </Chart>
        )
    }
}
