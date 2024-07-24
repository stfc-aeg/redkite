import './App.css';

import 'odin-react/dist/index.css'
import 'bootstrap/dist/css/bootstrap.min.css';

import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button';
import { InputGroup } from 'react-bootstrap';

import React from 'react';
import Alert from 'react-bootstrap/Alert';


import { TitleCard, ToggleSwitch, DropdownSelector, StatusBox, OdinApp } from 'odin-react';
import { WithEndpoint, useAdapterEndpoint } from 'odin-react';

const EndPointButton = WithEndpoint(Button);
const EndPointInput = WithEndpoint(Form.Control);

function formatData(data, indentLevel = 0) {
  const indent = '    '; // 4 spaces for indentation
  let formattedData = '';

  Object.entries(data).forEach(([key, value]) => {
    const prefix = `${indent.repeat(indentLevel)}${key}: `;
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      formattedData += `${prefix}\n${formatData(value, indentLevel + 1)}`;
    } else if (Array.isArray(value)) {
      const arrayString = value.map(item => JSON.stringify(item)).join(', ');
      formattedData += `${prefix}[${arrayString}]\n`;
    } else {
      formattedData += `${prefix}${JSON.stringify(value)}\n`;
    }
  });

  return formattedData;
}

function WSStatusBox({ label, type = 'success', children }) {
  return (
    <Alert variant={type} style={{ whiteSpace: 'pre-wrap' }}>
      <div>{label ? `${label}: ` : ''}{children}</div>
    </Alert>
  );
}

function App() {
  const munirEndpoint = useAdapterEndpoint('munir', 'http://localhost:8888', 1000)
  console.log(munirEndpoint.data);
  return (
    <OdinApp title="Odin Munir Adapter - Reactified" navLinks={['Frame Processor', 'Status']}>
        <TitleCard title="File Args">
          <Container>
              <Row>
                <Col>
                  <InputGroup>
                    <InputGroup.Text>Set File Path: </InputGroup.Text>
                    <EndPointInput endpoint={munirEndpoint} event_type="change" fullpath="args/file_path" delay={3000}/>
                  </InputGroup>
                </Col>
                <Col>
                  <InputGroup>
                    <InputGroup.Text>Set File Name: </InputGroup.Text>
                    <EndPointInput endpoint={munirEndpoint} event_type="change" fullpath="args/file_name" delay={3000}/>
                  </InputGroup>
                </Col>
              </Row>
              <Row>
                <Col>
                  <InputGroup>
                    <InputGroup.Text>Set # Frames: </InputGroup.Text>
                    <EndPointInput endpoint={munirEndpoint} event_type="change" fullpath="args/num_frames" delay={3000}/>
                  </InputGroup>
                </Col>
                <Col>
                  <InputGroup>
                    <InputGroup.Text>Set # Batches: </InputGroup.Text>
                    <EndPointInput endpoint={munirEndpoint} event_type="change" fullpath="args/num_batches" delay={3000}/>
                  </InputGroup>
                </Col>
              </Row>
              <Row>
                <EndPointButton endpoint={munirEndpoint} event_type="click" fullpath="execute" value={true}>Execute</EndPointButton>
              </Row>
          </Container>
        </TitleCard>
        
        <TitleCard title="Status">
          {/* Iterate through the keys in the response */}
          {Object.keys(munirEndpoint.data.frame_procs?.status[0] || {}).map((key) => {
          // Get the value of the current key
          const value = munirEndpoint.data.frame_procs?.status[0][key];

          return (
            <WSStatusBox key={key} as="span" type="info" label={`${key}`}>
              {formatData(value)}
            </WSStatusBox>
          );
        })}
        </TitleCard>

    </OdinApp>
  );
};

export default App;