import * as React from "react"

export class Chart extends React.Component {
    render() {
        return (
            <svg
                style={{padding:this.props.padding+'px'}}
                width={this.props.width}
                height={this.props.height}
            >
                {this.props.children}
            </svg>
        )
    }
}
