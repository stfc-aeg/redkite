import './App.css';

import 'odin-react/dist/index.css'
import 'bootstrap/dist/css/bootstrap.min.css';

import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button';
import { InputGroup } from 'react-bootstrap';

import Alert from 'react-bootstrap/Alert';

import { TitleCard, StatusBox, OdinApp } from 'odin-react';
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

// Function to dynamically generate an array of components for each subsystem 
function generateSubsystemRows(subsystems, munirEndpoint) {
  // Handle cases where no subsystems data is provided
  if (!subsystems) {
    return (
      <StatusBox label="Error" type="danger">
        No Subsystems Detected
      </StatusBox>
    );
  }

  // Map through each subsystem and create an array of compinents
  return (
    <>
      {Object.keys(subsystems).map((subsystemName) => (
        <Col key={subsystemName} sm={12} className="mb-4">
          <TitleCard title={
              <div className="d-flex align-items-center">
                <span style={{ fontSize: '1.25rem'}}>{subsystemName} Acquisition Controls |  </span>
                
                <div className="ms-3 d-flex" style={{ gap: '15px' }}>
                <EndPointButton
                  endpoint={munirEndpoint}
                  event_type="click"
                  fullpath={`execute/${subsystemName}`}
                  value={true}
                  className="w-100">
                  Execute on {subsystemName}
                </EndPointButton>

                </div>
              </div>
            }>
            <Row>
              <Col sm={12} md={6} lg={4} xl={4} xxl={4}>
                <InputGroup>
                  <InputGroup.Text>Set File Path:</InputGroup.Text>
                  <EndPointInput
                    endpoint={munirEndpoint}
                    event_type="change"
                    fullpath={`subsystems/${subsystemName}/args/file_path`}
                    delay={3000}
                  />
                </InputGroup>
              </Col>
              <Col sm={12} md={6} lg={4} xl={4} xxl={4}>
                <InputGroup>
                  <InputGroup.Text>Set File Name:</InputGroup.Text>
                  <EndPointInput
                    endpoint={munirEndpoint}
                    event_type="change"
                    fullpath={`subsystems/${subsystemName}/args/file_name`}
                    delay={3000}
                  />
                </InputGroup>
              </Col>
              <Col sm={12} md={6} lg={4} xl={4} xxl={4}>
                <InputGroup>
                  <InputGroup.Text>Set # Frames:</InputGroup.Text>
                  <EndPointInput
                    endpoint={munirEndpoint}
                    event_type="change"
                    fullpath={`subsystems/${subsystemName}/args/num_frames`}
                    delay={3000}
                  />
                </InputGroup>
              </Col>
            </Row>
          </TitleCard>
        </Col>
      ))}
      {Object.keys(subsystems).length > 1 && (
        <Row className="pt-3 justify-content-center">
          <Col sm={12} md={6} lg={4}>
            <Button onClick={() => executeAllSubsystems(subsystems, munirEndpoint)} className="w-100">
              Execute on all subsystems
            </Button>
          </Col>
        </Row>
      )}
    </>
  );
}

// Function to execute all subsystems with a single PUT request for each
function executeAllSubsystems(subsystems, munirEndpoint) {
  Object.keys(subsystems).forEach((subsystemName) => {
    munirEndpoint.put({ [subsystemName]: true }, 'execute');
  });
}

function App() {
  const endpoint_url = process.env.NODE_ENV === 'development' ? process.env.REACT_APP_ENDPOINT_URL : window.location.origin;
  const munirEndpoint = useAdapterEndpoint('munir', endpoint_url, 1000)
  const subsystems = munirEndpoint.data?.subsystems || {};

  return (
    <OdinApp title="Munir | Odin-Data Control" navLinks={['Munir Subsystems', 'Status']}>

        <TitleCard title="Munir Subsystems">
          <Container>
          {generateSubsystemRows(subsystems, munirEndpoint)}
          </Container>
        </TitleCard>
        
        <TitleCard title="Status">
          {Object.keys(subsystems).map((subsystemName) => {
          const statusData = subsystems[subsystemName]?.frame_procs?.status || [{}];
          return (
            <TitleCard key={subsystemName} title={`${subsystemName} Status`}>
              {statusData.map((status, index) => (
                <Container key={index}>
                  {Object.keys(status || {}).map((key) => (
                    <WSStatusBox key={key} as="span" type="info" label={`${key}`}>
                      {formatData(status[key])}
                    </WSStatusBox>
                  ))}
                </Container>
              ))}
            </TitleCard>
            );
          })}
        </TitleCard>

    </OdinApp>
  );
};

export default App;


