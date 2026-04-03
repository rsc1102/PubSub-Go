import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const baseURL = __ENV.BASE_URL || 'http://localhost:8080';

const createTopicVUs = Number(__ENV.CREATE_TOPIC_VUS || 2);
const createSubscriptionVUs = Number(__ENV.CREATE_SUBSCRIPTION_VUS || 2);
const publishConsumeVUs = Number(__ENV.PUBLISH_CONSUME_VUS || 10);

const publishSuccess = new Rate('publish_success');
const consumeSuccess = new Rate('consume_success');
const messageMatch = new Rate('message_match');
const publishBackpressure = new Counter('publish_backpressure');

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
    publish_consume: {
      executor: 'constant-vus',
      exec: 'publishAndConsume',
      vus: publishConsumeVUs,
      duration: __ENV.PUBLISH_CONSUME_DURATION || '15s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<200'],
    publish_success: ['rate>0.99'],
    consume_success: ['rate>0.99'],
    message_match: ['rate>0.99'],
  },
};

export function setup() {
  const runID = `run-${Date.now()}`;

  for (let i = 1; i <= publishConsumeVUs; i++) {
    const topic = `${runID}-pc-topic-${i}`;
    const subscription = `${runID}-pc-sub-${i}`;

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

export function publishAndConsume(data) {
  const vu = exec.vu.idInTest;
  const iter = exec.scenario.iterationInTest;
  const topic = `${data.runID}-pc-topic-${vu}`;
  const subscription = `${data.runID}-pc-sub-${vu}`;
  const content = `${data.runID}-msg-${vu}-${iter}`;

  const publishResponse = postJSON('/publish', { topic, content });
  const publishOK = publishResponse.status === 201;
  publishSuccess.add(publishOK);
  check(publishResponse, {
    'publish status is 201': (res) => res.status === 201,
  });

  if (!publishOK) {
    const body = safeJSON(publishResponse);
    const isBackpressure =
      publishResponse.status === 429 &&
      typeof body.error === 'string' &&
      body.error.includes('full queues');

    if (isBackpressure) {
      publishBackpressure.add(1);
    }

    check(body, {
      'publish failure is queue backpressure': (payload) => isBackpressure,
    });
    consumeSuccess.add(false);
    messageMatch.add(false);
    return;
  }

  const consumeResponse = postJSON('/consume', { topic, subscription });
  const consumeOK = consumeResponse.status === 200;
  consumeSuccess.add(consumeOK);
  check(consumeResponse, {
    'consume status is 200': (res) => res.status === 200,
  });

  if (!consumeOK) {
    messageMatch.add(false);
    return;
  }

  const body = safeJSON(consumeResponse);
  const matched = check(body, {
    'consume returned expected message': (payload) => payload.message === content,
  });
  messageMatch.add(matched);
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
