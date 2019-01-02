import * as React from "react"
import * as d3 from 'd3'
import { Line } from './line'

export class Timeline extends React.Component {
    static defaultProps = {
        title: '',
        data: [],
        color: '#2BECF6',
        strokeWidth: 2,
        interpolate: d3.curveLinear
    }

    render() {
        const { xScale, yScale, strokeWidth, color, interpolate, data } = this.props

        const path = d3.line()
            .x((d) => {
                return xScale(d.x)
            })
            .y((d) => {
                return yScale(d.y)
            })
            .curve(interpolate)

        const makeCircle = (xpos, ypos) => {
            const translate = `translate(${xScale(xpos)}, ${yScale(ypos)})`

            return (
                <circle
                    r="7"
                    stroke={color}
                    strokeWidth={strokeWidth}
                    // fill="#F3F4F5"
                    fill={color}
                    transform={translate}
                />
            )
        }

        const circles = data.map((point) => {
            return makeCircle(point.x, point.y)
        })

        return (
            <g>
                <Line path={path(this.props.data)} color={this.props.color} width={this.props.strokeWidth}/>
                {circles}
            </g>
        )
    }
}
