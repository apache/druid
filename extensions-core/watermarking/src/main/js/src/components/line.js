import * as React from "react"

export class Line extends React.Component {
    render() {
        return(
            <path d={this.props.path} stroke={this.props.color} strokeWidth={this.props.width} fill="none" />
        )
    }
}
