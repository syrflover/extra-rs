use std::{
    collections::{HashMap, hash_map},
    future::IntoFuture,
    hash::Hash,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use either::Either;
use futures::{StreamExt, stream};
use tokio::{sync::mpsc, task::JoinHandle, time::sleep};

struct TaskSchedulerArguments {
    stop_tx: mpsc::Sender<()>,
    handle: JoinHandle<()>,
    interval: Duration,
    expiration: DateTime<Utc>,
    payload: Arc<Bytes>,
}

#[derive(Default)]
pub struct TaskScheduler<K>
where
    K: Clone + Hash + PartialEq + Eq,
{
    store: HashMap<K, TaskSchedulerArguments>,
}

impl<K> TaskScheduler<K>
where
    K: Clone + Hash + PartialEq + Eq,
{
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.store.contains_key(key)
    }

    #[allow(private_interfaces)]
    pub fn keys(&self) -> hash_map::Keys<'_, K, TaskSchedulerArguments> {
        self.store.keys()
    }

    pub fn get_expiration(&self, key: &K) -> Option<DateTime<Utc>> {
        self.store.get(key).map(|x| x.expiration)
    }

    /// returns None if exists key
    ///
    /// ## 주의
    /// - **rx의 연결이 끊기면 `remove(&key)`를 꼭 호출해 주세요**
    ///
    ///   제거하지 않으면 계속 남아있어요
    ///
    /// - 수신한 payload를 처리하는 시간이 interval보다 짧으면 연속적으로 값을 받게 되고,
    ///   저장 가능한 최대 메세지 개수 제한이 없으므로, expiration 시간이 길고 interval이 짧은 경우에는 메모리 소모가 커질 수 있음
    ///
    ///   예를 들면, interval은 1초인데 payload를 처리하는 함수의 실행 시간이 3초면, 함수 실행이 끝나자마자 바로 다음 값을 받게 됨
    pub fn insert(
        &mut self,
        key: K,
        interval: Duration,
        expiration: Duration,
        payload: Bytes,
    ) -> Option<mpsc::UnboundedReceiver<Arc<Bytes>>> {
        insert(
            &mut self.store,
            key,
            interval,
            Either::Left(expiration),
            payload,
        )
    }

    pub async fn remove(&mut self, key: &K) -> Option<(Duration, DateTime<Utc>, Bytes)> {
        match self.store.remove(key) {
            Some(args) => Some(remove(args).await),
            None => None,
        }
    }

    pub async fn drain_hashmap(
        &mut self,
        concurrency: usize,
    ) -> HashMap<K, (Duration, DateTime<Utc>, Bytes)> {
        stream::iter(self.store.drain())
            .map(|(key, args)| async move { (key, remove(args).await) })
            .buffer_unordered(concurrency)
            .collect::<HashMap<_, _>>()
            .await
    }

    #[allow(clippy::type_complexity)]
    pub fn from_hashmap(
        hashmap: HashMap<K, (Duration, DateTime<Utc>, Bytes)>,
    ) -> (Self, Vec<(K, mpsc::UnboundedReceiver<Arc<Bytes>>)>) {
        let mut task_scheduler = Self::new();

        let now = Utc::now();

        let rxs = hashmap
            .into_iter()
            .filter(|(_, (interval, expiration, _))| *expiration > now + *interval)
            .filter_map(|(key, (interval, expiration, payload))| {
                insert(
                    &mut task_scheduler.store,
                    key.clone(),
                    interval,
                    Either::Right(expiration),
                    payload,
                )
                .map(|rx| (key, rx))
            })
            .collect::<Vec<_>>();

        (task_scheduler, rxs)
    }
}

fn insert<K>(
    store: &mut HashMap<K, TaskSchedulerArguments>,
    key: K,
    interval: Duration,
    expiration: Either<Duration, DateTime<Utc>>,
    payload: Bytes,
) -> Option<mpsc::UnboundedReceiver<Arc<Bytes>>>
where
    K: Hash + PartialEq + Eq,
{
    if store.contains_key(&key) {
        return None;
    }

    let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
    let (tx, rx) = mpsc::unbounded_channel::<Arc<Bytes>>();

    let expiration = match expiration {
        Either::Left(expiration) => Utc::now() + expiration,
        Either::Right(expiration) => expiration,
    };
    let payload = Arc::new(payload);

    let handle = tokio::spawn({
        let payload = payload.clone();
        async move {
            loop {
                tokio::select! {
                    _ = sleep(interval) => {}
                    _ = stop_rx.recv() => {
                        return;
                    }
                }

                if Utc::now() < expiration {
                    if tx.send(payload.clone()).is_err() {
                        return;
                    }
                } else {
                    return;
                }
            }
        }
    });

    store.insert(
        key,
        TaskSchedulerArguments {
            stop_tx,
            handle,
            interval,
            expiration,
            payload,
        },
    );

    Some(rx)
}

async fn remove(
    TaskSchedulerArguments {
        stop_tx,
        handle,
        interval,
        expiration,
        payload,
    }: TaskSchedulerArguments,
) -> (Duration, DateTime<Utc>, Bytes) {
    stop_tx.send(()).await.ok();
    handle.into_future().await.ok();

    (interval, expiration, (*payload).clone())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{StreamExt, stream};
    use once_cell::sync::Lazy;
    use tokio::sync::Mutex;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    enum Key {
        A(u32),
        B(u32),
        C(u32),
    }

    impl Key {
        fn inner(&self) -> u32 {
            match self {
                Key::A(r) => *r,
                Key::B(r) => *r,
                Key::C(r) => *r,
            }
        }
    }

    static TASK_SCHEDULER: Lazy<Arc<Mutex<TaskScheduler<Key>>>> =
        Lazy::new(|| Arc::new(Mutex::new(TaskScheduler::<Key>::new())));

    #[tokio::test]
    async fn consume_all() {
        let expected_payload = b"payload";

        stream::iter(0..30)
            .map(|i| {
                let task_scheduler = TASK_SCHEDULER.clone();
                async move {
                    let mut rx = task_scheduler
                        .lock()
                        .await
                        .insert(
                            Key::A(i),
                            Duration::from_millis(100),
                            Duration::from_secs(1),
                            Bytes::copy_from_slice(expected_payload),
                        )
                        .unwrap();

                    let mut recv_count = 0;

                    while let Some(payload) = rx.recv().await {
                        recv_count += 1;
                        assert_eq!((*payload).as_ref(), expected_payload);
                    }

                    assert_eq!(recv_count, 9);
                }
            })
            .buffer_unordered(30)
            .collect::<()>()
            .await;
    }

    #[tokio::test]
    async fn remove_all() {
        let expected_payload = b"payload";

        stream::iter(0..30)
            .map(|i| {
                let task_scheduler = TASK_SCHEDULER.clone();
                async move {
                    let key = Key::B(i);

                    let mut rx = task_scheduler
                        .lock()
                        .await
                        .insert(
                            key,
                            Duration::from_millis(100),
                            Duration::from_secs(1),
                            Bytes::copy_from_slice(expected_payload),
                        )
                        .unwrap();

                    let mut recv_count = 0;

                    while let Some(payload) = rx.recv().await {
                        recv_count += 1;
                        assert_eq!((*payload).as_ref(), expected_payload);

                        if recv_count >= 5 {
                            let mut task_scheduler = task_scheduler.lock().await;
                            task_scheduler.remove(&key).await;
                        }
                    }

                    assert_eq!(recv_count, 5);
                }
            })
            .buffer_unordered(30)
            .collect::<()>()
            .await;
    }

    #[tokio::test]
    async fn consume_all_with_from_hashmap() {
        let expected_payload = b"payload";

        let task_scheduler = Arc::new(Mutex::new(TaskScheduler::<Key>::new()));

        let (_, data) = tokio::join!(
            stream::iter(1..=30)
                .map(|i| {
                    let task_scheduler = task_scheduler.clone();
                    async move {
                        let key = Key::C(i);

                        let mut rx = task_scheduler
                            .lock()
                            .await
                            .insert(
                                key,
                                Duration::from_millis(100),
                                Duration::from_millis((100 * i + 1) as u64),
                                Bytes::copy_from_slice(expected_payload),
                            )
                            .unwrap();

                        let mut recv_count = 0;

                        while let Some(payload) = rx.recv().await {
                            recv_count += 1;
                            assert_eq!((*payload).as_ref(), expected_payload);
                        }

                        let expected_recv_count = if 100 * i + 1 >= 800 { 7 } else { i - 1 };

                        assert_eq!(recv_count, expected_recv_count);
                    }
                })
                .buffer_unordered(30)
                .collect::<()>(),
            async {
                sleep(Duration::from_millis(800)).await;
                let mut task_scheduler = task_scheduler.lock().await;
                task_scheduler.drain_hashmap(30).await
            }
        );

        assert_eq!(data.len(), 30);

        let (task_scheduler, rxs) = TaskScheduler::from_hashmap(data);

        let task_scheduler = Arc::new(Mutex::new(task_scheduler));

        stream::iter(rxs)
            .map(|(key, mut rx)| {
                let task_scheduler = task_scheduler.clone();
                async move {
                    let expiration = task_scheduler.lock().await.get_expiration(&key).unwrap();
                    let now = Utc::now();
                    let expected_recv_count = if expiration > now {
                        println!(
                            "{}: {}",
                            key.inner(),
                            ((expiration.timestamp_millis() - now.timestamp_millis()) as f64
                                / 100.0)
                        );
                        match expiration.timestamp_millis() - now.timestamp_millis() {
                            x @ 100.. => {
                                let low = (x / 100) - 1;
                                let high = x / 100;
                                Some(low..=high)
                            }

                            _ => None,
                        }
                    } else {
                        None
                    }
                    .unwrap();

                    let mut actual_recv_count = 0;

                    while let Some(payload) = rx.recv().await {
                        if actual_recv_count == 0 {
                            println!(
                                "{}: {}",
                                key.inner(),
                                Utc::now().timestamp_millis() - now.timestamp_millis()
                            );
                        }

                        actual_recv_count += 1;
                        assert_eq!((*payload).as_ref(), expected_payload);
                    }

                    println!(
                        "{}: actual={}; expected={:?}",
                        key.inner(),
                        actual_recv_count,
                        expected_recv_count
                    );

                    assert!(
                        expected_recv_count.contains(&actual_recv_count),
                        "{}: actual={}; expected={:?}",
                        key.inner(),
                        actual_recv_count,
                        expected_recv_count
                    );
                }
            })
            .buffer_unordered(30)
            .collect::<()>()
            .await;
    }
}
