import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const baseURL = __ENV.BASE_URL || 'http://localhost:8080';

const createTopicVUs = Number(__ENV.CREATE_TOPIC_VUS || 2);
const createSubscriptionVUs = Number(__ENV.CREATE_SUBSCRIPTION_VUS || 2);
const publishDeliveryVUs = Number(__ENV.PUBLISH_DELIVERY_VUS || 10);
const totalScenarioVUs =
  createTopicVUs + createSubscriptionVUs + publishDeliveryVUs;

const publishSuccess = new Rate('publish_success');
const publishDeliveryLatency = new Trend('publish_delivery_latency', true);
const publishBackpressure = new Counter('publish_backpressure');
const publishTopicMissing = new Counter('publish_topic_missing');
const publishUnexpectedFailure = new Counter('publish_unexpected_failure');

export const options = {
  scenarios: {
    create_topics: {
      executor: 'constant-vus',
      exec: 'createTopics',
      vus: createTopicVUs,
      duration: __ENV.CREATE_TOPIC_DURATION || '10s',
    },
    create_subscriptions: {
      executor: 'constant-vus',
      exec: 'createSubscriptions',
      vus: createSubscriptionVUs,
      duration: __ENV.CREATE_SUBSCRIPTION_DURATION || '10s',
    },
    publish_delivery: {
      executor: 'constant-vus',
      exec: 'publishAndMeasureDeliveryBuffer',
      vus: publishDeliveryVUs,
      duration: __ENV.PUBLISH_DELIVERY_DURATION || '15s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<200'],
    publish_success: ['rate>0.99'],
  },
};

export function setup() {
  const runID = `run-${Date.now()}`;

  // idInTest is global across scenarios, so pre-create enough publish/delivery
  // resources to cover the full VU id range used by this mixed-scenario run.
  for (let i = 1; i <= totalScenarioVUs; i++) {
    const topic = `${runID}-pd-topic-${i}`;
    const subscription = `${runID}-pd-sub-${i}`;

    const createTopicResponse = postJSON('/topics', { topic });
    assertStatus(createTopicResponse, 201, 'setup create topic');

    const createSubscriptionResponse = postJSON('/subscriptions', {
      topic,
      subscription,
    });
    assertStatus(createSubscriptionResponse, 201, 'setup create subscription');
  }

  return { runID };
}

export function createTopics(data) {
  const vu = exec.vu.idInTest;
  const iter = exec.scenario.iterationInTest;
  const topic = `${data.runID}-ct-topic-${vu}-${iter}`;

  const response = postJSON('/topics', { topic });
  assertStatus(response, 201, 'create topic');
}

export function createSubscriptions(data) {
  const vu = exec.vu.idInTest;
  const iter = exec.scenario.iterationInTest;
  const topic = `${data.runID}-cs-topic-${vu}-${iter}`;
  const subscription = `${data.runID}-cs-sub-${vu}-${iter}`;

  const createTopicResponse = postJSON('/topics', { topic });
  assertStatus(createTopicResponse, 201, 'create topic for subscription');

  const createSubscriptionResponse = postJSON('/subscriptions', {
    topic,
    subscription,
  });
  assertStatus(createSubscriptionResponse, 201, 'create subscription');
}

export function publishAndMeasureDeliveryBuffer(data) {
  const vu = exec.vu.idInTest;
  const iter = exec.scenario.iterationInTest;
  const topic = `${data.runID}-pd-topic-${vu}`;
  const content = `${data.runID}-msg-${vu}-${iter}`;
  const startedAt = Date.now();

  const publishResponse = postJSON('/publish', { topic, content });
  const publishOK = publishResponse.status === 201;
  publishSuccess.add(publishOK);
  check(publishResponse, {
    'publish status is 201': (res) => res.status === 201,
  });

  if (!publishOK) {
    const body = safeJSON(publishResponse);
    const errorMessage = typeof body.error === 'string' ? body.error : '';
    const isBackpressure =
      publishResponse.status === 429 &&
      errorMessage.includes('full queues');
    const isTopicMissing =
      publishResponse.status === 400 &&
      errorMessage.includes('topic not found');

    if (isBackpressure) {
      publishBackpressure.add(1);
    }
    if (isTopicMissing) {
      publishTopicMissing.add(1);
    }
    if (!isBackpressure && !isTopicMissing) {
      publishUnexpectedFailure.add(1);
    }

    check(body, {
      'publish failure is queue backpressure': (payload) => isBackpressure,
      'publish failure is missing topic': (payload) => isTopicMissing,
      'publish failure is unexpected': (payload) =>
        !isBackpressure && !isTopicMissing,
    });
    return;
  }

  publishDeliveryLatency.add(Date.now() - startedAt);
}

function postJSON(path, payload) {
  return http.post(`${baseURL}${path}`, JSON.stringify(payload), {
    headers: { 'Content-Type': 'application/json' },
  });
}

function assertStatus(response, expectedStatus, name) {
  check(response, {
    [`${name} status is ${expectedStatus}`]: (res) => res.status === expectedStatus,
  });
}

function safeJSON(response) {
  try {
    return response.json();
  } catch (_) {
    return {};
  }
}
