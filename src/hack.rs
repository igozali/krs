// TODO: Remove when https://github.com/fede1024/rust-rdkafka/issues/179 is resolved
// This file is more or less a copy-paste from rdkafka's AdminClient code
// with only the functions I want and a lot of hacks to get it to work with
// my crate.
use rdkafka_sys as rdsys;
use rdsys::bindings;
use rdsys::types::*;

use log::*;

use rdkafka::client::{Client, ClientContext, DefaultClientContext};
use rdkafka::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use rdkafka::error::{IsError, KafkaResult};
use rdkafka::util::{cstr_to_owned, IntoOpaque};

use futures::future::{self, Either};
use futures::{Async, Canceled, Complete, Future, Oneshot, Poll};

use std::os::raw::c_char;
use std::ffi::{CStr, CString};
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Specifies a timeout for a Kafka operation.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Timeout {
    /// Time out after the specified duration elapses.
    After(Duration),
    /// Block forever.
    Never,
}

impl Timeout {
    /// Converts a timeout to Kafka's expected representation.
    pub(crate) fn as_millis(&self) -> i32 {
        match self {
            Timeout::After(d) => d.as_millis() as i32,
            Timeout::Never => -1,
        }
    }
}

impl std::ops::SubAssign for Timeout {
    fn sub_assign(&mut self, other: Self) {
        match (self, other) {
            (Timeout::After(lhs), Timeout::After(rhs)) => *lhs -= rhs,
            (Timeout::Never, Timeout::After(_)) => (),
            _ => panic!("subtraction of Timeout::Never is ill-defined"),
        }
    }
}

impl From<Duration> for Timeout {
    fn from(d: Duration) -> Timeout {
        Timeout::After(d)
    }
}

impl From<Option<Duration>> for Timeout {
    fn from(v: Option<Duration>) -> Timeout {
        match v {
            None => Timeout::Never,
            Some(d) => Timeout::After(d),
        }
    }
}

pub(crate) fn timeout_to_ms<T: Into<Option<Duration>>>(timeout: T) -> i32 {
    timeout
        .into()
        .map(|t| t.as_millis() as i32)
        .unwrap_or(-1)
}


pub unsafe fn bytes_cstr_to_owned(bytes_cstr: &[i8]) -> String {
    CStr::from_ptr(bytes_cstr.as_ptr() as *const c_char).to_string_lossy().into_owned()
}

pub(crate) trait WrappedCPointer {
    type Target;

    fn ptr(&self) -> *mut Self::Target;

    fn is_null(&self) -> bool {
        self.ptr().is_null()
    }
}

/// Converts a container into a C array.
pub(crate) trait AsCArray<T: WrappedCPointer> {
    fn as_c_array(&self) -> *mut *mut T::Target;
}

impl<T: WrappedCPointer> AsCArray<T> for Vec<T> {
    fn as_c_array(&self) -> *mut *mut T::Target {
        self.as_ptr() as *mut *mut T::Target
    }
}


pub(crate) struct NativeQueue {
    ptr: *mut RDKafkaQueue
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeQueue {}
unsafe impl Send for NativeQueue {}

impl NativeQueue {
    /// Wraps a pointer to an `RDKafkaQueue` object and returns a new
    /// `NativeQueue`.
    unsafe fn from_ptr(ptr: *mut RDKafkaQueue) -> NativeQueue {
        NativeQueue { ptr }
    }

    /// Returns the pointer to the librdkafka RDKafkaQueue structure.
    pub fn ptr(&self) -> *mut RDKafkaQueue {
        self.ptr
    }

    pub fn poll<T: Into<Option<Duration>>>(&self, t: T) -> *mut RDKafkaEvent {
        unsafe { rdsys::rd_kafka_queue_poll(self.ptr, timeout_to_ms(t)) }
    }
}

impl Drop for NativeQueue {
    fn drop(&mut self) {
        trace!("Destroying queue: {:?}", self.ptr);
        unsafe {
            rdsys::rd_kafka_queue_destroy(self.ptr);
        }
        trace!("Queue destroyed: {:?}", self.ptr);
    }
}

pub(crate) struct ErrBuf {
    buf: [c_char; ErrBuf::MAX_ERR_LEN]
}

impl ErrBuf {
    const MAX_ERR_LEN: usize = 512;

    pub fn new() -> ErrBuf {
        ErrBuf { buf: [0; ErrBuf::MAX_ERR_LEN] }
    }

    pub fn as_mut_ptr(&mut self) -> *mut i8 {
        self.buf.as_mut_ptr()
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn to_string(&self) -> String {
        unsafe { bytes_cstr_to_owned(&self.buf) }
    }
}

impl Default for ErrBuf {
    fn default() -> ErrBuf {
        ErrBuf::new()
    }
}

// Native rdkafka queue
pub type RDKafkaQueue = bindings::rd_kafka_queue_t;

// Native rdkafka new topic object
pub type RDKafkaNewTopic = bindings::rd_kafka_NewTopic_t;

// Native rdkafka delete topic object
pub type RDKafkaDeleteTopic = bindings::rd_kafka_DeleteTopic_t;

// Native rdkafka event
pub type RDKafkaEvent = bindings::rd_kafka_event_t;

// Native rdkafka admin options
pub type RDKafkaAdminOptions = bindings::rd_kafka_AdminOptions_t;

// Native rdkafka topic result
pub type RDKafkaTopicResult = bindings::rd_kafka_topic_result_t;

/// Admin operation
pub use bindings::rd_kafka_admin_op_t as RDKafkaAdminOp;

/// A client for the Kafka admin API.
///
/// `AdminClient` provides programmatic access to managing a Kafka cluster,
/// notably manipulating topics, partitions, and configuration paramaters.
pub struct AdminClient<C: ClientContext> {
    client: Client<C>,
    queue: Arc<NativeQueue>,
    should_stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl<C: ClientContext> AdminClient<C> {
    /// Creates new topics according to the provided `NewTopic` specifications.
    ///
    /// Note that while the API supports creating multiple topics at once, it
    /// is not transactional. Creation of some topics may succeed while others
    /// fail. Be sure to check the result of each individual operation.
    pub fn create_topics<'a, I>(
        &self,
        topics: I,
        opts: &AdminOptions,
    ) -> impl Future<Item = Vec<TopicResult>, Error = crate::Error>
    where
        I: IntoIterator<Item = &'a NewTopic<'a>>,
    {
        match self.create_topics_inner(topics, opts) {
            Ok(rx) => Either::A(CreateTopicsFuture { rx }),
            Err(err) => Either::B(future::err(err)),
        }
    }

    fn create_topics_inner<'a, I>(
        &self,
        topics: I,
        opts: &AdminOptions,
    ) -> crate::Result<Oneshot<NativeEvent>>
    where
        I: IntoIterator<Item = &'a NewTopic<'a>>,
    {
        let mut native_topics = Vec::new();
        let mut err_buf = ErrBuf::new();
        for t in topics {
            native_topics.push(t.to_native(&mut err_buf)?);
        }
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        unsafe {
            rdsys::rd_kafka_CreateTopics(
                self.client.native_ptr(),
                native_topics.as_c_array(),
                native_topics.len(),
                native_opts.ptr(),
                self.queue.ptr(),
            );
        }
        Ok(rx)
    }

    /// Deletes the named topics.
    ///
    /// Note that while the API supports deleting multiple topics at once, it is
    /// not transactional. Deletion of some topics may succeed while others
    /// fail. Be sure to check the result of each individual operation.
    pub fn delete_topics(
        &self,
        topic_names: &[&str],
        opts: &AdminOptions,
    ) -> impl Future<Item = Vec<TopicResult>, Error = crate::Error> {
        match self.delete_topics_inner(topic_names, opts) {
            Ok(rx) => Either::A(DeleteTopicsFuture { rx }),
            Err(err) => Either::B(future::err(err)),
        }
    }

    fn delete_topics_inner(
        &self,
        topic_names: &[&str],
        opts: &AdminOptions,
    ) -> crate::Result<Oneshot<NativeEvent>> {
        let mut native_topics = Vec::new();
        let mut err_buf = ErrBuf::new();
        for tn in topic_names {
            let tn_c = CString::new(*tn)?;
            let native_topic = unsafe {
                NativeDeleteTopic::from_ptr(rdsys::rd_kafka_DeleteTopic_new(tn_c.as_ptr()))
            };
            native_topics.push(native_topic);
        }
        let (native_opts, rx) = opts.to_native(self.client.native_ptr(), &mut err_buf)?;
        unsafe {
            rdsys::rd_kafka_DeleteTopics(
                self.client.native_ptr(),
                native_topics.as_c_array(),
                native_topics.len(),
                native_opts.ptr(),
                self.queue.ptr(),
            );
        }
        Ok(rx)
    }
}

impl FromClientConfig for AdminClient<DefaultClientContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<AdminClient<DefaultClientContext>> {
        AdminClient::from_config_and_context(config, DefaultClientContext)
    }
}

fn new_native_queue<C: ClientContext>(client: &Client<C>) -> NativeQueue {
    unsafe {
        NativeQueue::from_ptr(rdsys::rd_kafka_queue_new(client.native_ptr()))
    }
}

impl<C: ClientContext> FromClientConfigAndContext<C> for AdminClient<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<AdminClient<C>> {
        let native_config = config.create_native_config()?;
        // librdkafka only provides consumer and producer types. We follow the
        // example of the Python bindings in choosing to pretend to be a
        // producer, as producer clients are allegedly more lightweight. [0]
        //
        // [0]: https://github.com/confluentinc/confluent-kafka-python/blob/bfb07dfbca47c256c840aaace83d3fe26c587360/confluent_kafka/src/Admin.c#L1492-L1493
        let client = Client::new(
            config,
            native_config,
            RDKafkaType::RD_KAFKA_PRODUCER,
            context,
        )?;
        let queue = Arc::new(new_native_queue(&client));
        let should_stop = Arc::new(AtomicBool::new(false));
        let handle = start_poll_thread(queue.clone(), should_stop.clone());
        Ok(AdminClient {
            client,
            queue,
            should_stop,
            handle: Some(handle),
        })
    }
}

impl<C: ClientContext> Drop for AdminClient<C> {
    fn drop(&mut self) {
        trace!("Stopping polling");
        self.should_stop.store(true, Ordering::Relaxed);
        trace!("Waiting for polling thread termination");
        match self.handle.take().unwrap().join() {
            Ok(()) => trace!("Polling stopped"),
            Err(e) => warn!("Failure while terminating thread: {:?}", e),
        };
    }
}

fn start_poll_thread(queue: Arc<NativeQueue>, should_stop: Arc<AtomicBool>) -> JoinHandle<()> {
    thread::Builder::new()
        .name("admin client polling thread".into())
        .spawn(move || {
            trace!("Admin polling thread loop started");
            loop {
                let event = queue.poll(Duration::from_millis(100));
                if event.is_null() {
                    if should_stop.load(Ordering::Relaxed) {
                        // We received nothing and the thread should stop, so
                        // break the loop.
                        break;
                    }
                    continue;
                }
                let event = unsafe { NativeEvent::from_ptr(event) };
                let tx: Box<Complete<NativeEvent>> =
                    unsafe { IntoOpaque::from_ptr(rdsys::rd_kafka_event_opaque(event.ptr())) };
                let _ = tx.send(event);
            }
            trace!("Admin polling thread loop terminated");
        })
        .expect("Failed to start polling thread")
}

struct NativeEvent {
    ptr: *mut RDKafkaEvent,
}

impl NativeEvent {
    unsafe fn from_ptr(ptr: *mut RDKafkaEvent) -> NativeEvent {
        NativeEvent { ptr }
    }

    fn ptr(&self) -> *mut RDKafkaEvent {
        self.ptr
    }

    fn check_error(&self) -> crate::Result<()> {
        let err = unsafe { rdsys::rd_kafka_event_error(self.ptr) };
        if err.is_error() {
            Err(crate::Error::RDKafka(err.into()))
        } else {
            Ok(())
        }
    }
}

impl Drop for NativeEvent {
    fn drop(&mut self) {
        trace!("Destroying event: {:?}", self.ptr);
        unsafe {
            rdsys::rd_kafka_event_destroy(self.ptr);
        }
        trace!("Event destroyed: {:?}", self.ptr);
    }
}

unsafe impl Sync for NativeEvent {}
unsafe impl Send for NativeEvent {}

//
// ********** ADMIN OPTIONS **********
//

const RD_KAFKA_ADMIN_OP_ANY: u32 = 0;

/// Options for an admin API request.
pub struct AdminOptions {
    request_timeout: Option<Timeout>,
    operation_timeout: Option<Timeout>,
    validate_only: bool,
    broker_id: Option<i32>,
}

impl AdminOptions {
    /// Creates a new `AdminOptions`.
    pub fn new() -> AdminOptions {
        AdminOptions {
            request_timeout: None,
            operation_timeout: None,
            validate_only: false,
            broker_id: None,
        }
    }

    /// Sets the overall request timeout, including broker lookup, request
    /// transmission, operation time on broker, and response.
    ///
    /// Defaults to the `socket.timeout.ms` configuration parameter.
    #[allow(dead_code)]
    pub fn request_timeout<T: Into<Timeout>>(mut self, timeout: Option<T>) -> Self {
        self.request_timeout = timeout.map(Into::into);
        self
    }

    /// Sets the broker's operation timeout, such as the timeout for
    /// CreateTopics to complete the creation of topics on the controller before
    /// returning a result to the application.
    ///
    /// If unset (the default), the API calls will return immediately after
    /// triggering the operation.
    ///
    /// Only the CreateTopics, DeleteTopics, and CreatePartitions API calls
    /// respect this option.
    #[allow(dead_code)]
    pub fn operation_timeout<T: Into<Timeout>>(mut self, timeout: Option<T>) -> Self {
        self.operation_timeout = timeout.map(Into::into);
        self
    }

    /// Tells the broker to only validate the request, without performing the
    /// requested operation.
    ///
    /// Defaults to false.
    #[allow(dead_code)]
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Override what broker the admin request will be sent to.
    ///
    /// By default, a reasonable broker will be selected automatically. See the
    /// librdkafka docs on `rd_kafka_AdminOptions_set_broker` for details.
    #[allow(dead_code)]
    pub fn broker_id<T: Into<Option<i32>>>(mut self, broker_id: T) -> Self {
        self.broker_id = broker_id.into();
        self
    }

    fn to_native(
        &self,
        client: *mut RDKafka,
        err_buf: &mut ErrBuf,
    ) -> crate::Result<(NativeAdminOptions, Oneshot<NativeEvent>)> {
        let native_opts = unsafe {
            NativeAdminOptions::from_ptr(rdsys::rd_kafka_AdminOptions_new(
                client,
                RD_KAFKA_ADMIN_OP_ANY,
            ))
        };

        if let Some(timeout) = self.request_timeout {
            let res = unsafe {
                rdsys::rd_kafka_AdminOptions_set_request_timeout(
                    native_opts.ptr(),
                    timeout.as_millis(),
                    err_buf.as_mut_ptr(),
                    err_buf.len(),
                )
            };
            check_rdkafka_invalid_arg(res, err_buf)?;
        }

        if let Some(timeout) = self.operation_timeout {
            let res = unsafe {
                rdsys::rd_kafka_AdminOptions_set_operation_timeout(
                    native_opts.ptr(),
                    timeout.as_millis(),
                    err_buf.as_mut_ptr(),
                    err_buf.len(),
                )
            };
            check_rdkafka_invalid_arg(res, err_buf)?;
        }

        if self.validate_only {
            let res = unsafe {
                rdsys::rd_kafka_AdminOptions_set_validate_only(
                    native_opts.ptr(),
                    1, // true
                    err_buf.as_mut_ptr(),
                    err_buf.len(),
                )
            };
            check_rdkafka_invalid_arg(res, err_buf)?;
        }

        if let Some(broker_id) = self.broker_id {
            let res = unsafe {
                rdsys::rd_kafka_AdminOptions_set_broker(
                    native_opts.ptr(),
                    broker_id,
                    err_buf.as_mut_ptr(),
                    err_buf.len(),
                )
            };
            check_rdkafka_invalid_arg(res, err_buf)?;
        }

        let (tx, rx) = futures::oneshot();
        let tx = Box::new(tx);
        unsafe {
            rdsys::rd_kafka_AdminOptions_set_opaque(native_opts.ptr, IntoOpaque::as_ptr(&tx))
        };
        mem::forget(tx);

        Ok((native_opts, rx))
    }
}

struct NativeAdminOptions {
    ptr: *mut RDKafkaAdminOptions,
}

impl NativeAdminOptions {
    unsafe fn from_ptr(ptr: *mut RDKafkaAdminOptions) -> NativeAdminOptions {
        NativeAdminOptions { ptr }
    }

    fn ptr(&self) -> *mut RDKafkaAdminOptions {
        self.ptr
    }
}

impl Drop for NativeAdminOptions {
    fn drop(&mut self) {
        trace!("Destroying admin options: {:?}", self.ptr);
        unsafe {
            rdsys::rd_kafka_AdminOptions_destroy(self.ptr);
        }
        trace!("Admin options destroyed: {:?}", self.ptr);
    }
}

fn check_rdkafka_invalid_arg(res: RDKafkaRespErr, err_buf: &ErrBuf) -> crate::Result<()> {
    match res.into() {
        RDKafkaError::NoError => Ok(()),
        RDKafkaError::InvalidArgument => {
            let msg = if err_buf.len() == 0 {
                "invalid argument".into()
            } else {
                err_buf.to_string()
            };
            Err(crate::Error::Generic(msg))
        }
        res => Err(crate::Error::Generic(format!(
            "setting admin options returned unexpected error code {}",
            res
        ))),
    }
}

//
// ********** RESPONSE HANDLING **********
//

/// The result of an individual CreateTopic, DeleteTopic, or
/// CreatePartition operation.
pub type TopicResult = Result<String, (String, RDKafkaError)>;

fn build_topic_results(topics: *const *const RDKafkaTopicResult, n: usize) -> Vec<TopicResult> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let topic = unsafe { *topics.offset(i as isize) };
        let name = unsafe { cstr_to_owned(rdsys::rd_kafka_topic_result_name(topic)) };
        let err = unsafe { rdsys::rd_kafka_topic_result_error(topic) };
        if err.is_error() {
            out.push(Err((name, err.into())));
        } else {
            out.push(Ok(name));
        }
    }
    out
}

impl From<std::ffi::NulError> for crate::Error {
    fn from(err: std::ffi::NulError) -> Self {
        crate::Error::Other(Box::new(err))
    }
}


//
// Create topic handling
//

/// Configuration for a CreateTopic operation.
#[derive(Debug)]
pub struct NewTopic<'a> {
    /// The name of the new topic.
    pub name: &'a str,
    /// The initial number of partitions.
    pub num_partitions: i32,
    /// The initial replication configuration.
    pub replication: TopicReplication<'a>,
    /// The initial configuration parameters for the topic.
    pub config: Vec<(&'a str, &'a str)>,
}

impl<'a> NewTopic<'a> {
    /// Creates a new `NewTopic`.
    pub fn new(
        name: &'a str,
        num_partitions: i32,
        replication: TopicReplication<'a>,
    ) -> NewTopic<'a> {
        NewTopic {
            name,
            num_partitions,
            replication,
            config: Vec::new(),
        }
    }

    /// Sets a new parameter in the initial topic configuration.
    #[allow(dead_code)]
    pub fn set(mut self, key: &'a str, value: &'a str) -> NewTopic<'a> {
        self.config.push((key, value));
        self
    }

    fn to_native(&self, err_buf: &mut ErrBuf) -> crate::Result<NativeNewTopic> {
        let name = CString::new(self.name)?;
        let repl = match self.replication {
            TopicReplication::Fixed(n) => n,
            TopicReplication::Variable(partitions) => {
                if partitions.len() as i32 != self.num_partitions {
                    return Err(crate::Error::Generic(format!(
                        "replication configuration for topic '{}' assigns {} partition(s), \
                         which does not match the specified number of partitions ({})",
                        self.name,
                        partitions.len(),
                        self.num_partitions,
                    )));
                }
                -1
            }
        };
        let topic = unsafe {
            rdsys::rd_kafka_NewTopic_new(
                name.as_ptr(),
                self.num_partitions,
                repl,
                err_buf.as_mut_ptr(),
                err_buf.len(),
            )
        };
        if topic.is_null() {
            return Err(crate::Error::Generic(err_buf.to_string()));
        }
        // N.B.: we wrap topic immediately, so that it is destroyed via the
        // NativeNewTopic's Drop implementation if replica assignment or config
        // installation fails.
        let topic = unsafe { NativeNewTopic::from_ptr(topic) };
        if let TopicReplication::Variable(assignment) = self.replication {
            for (partition_id, broker_ids) in assignment.into_iter().enumerate() {
                let res = unsafe {
                    rdsys::rd_kafka_NewTopic_set_replica_assignment(
                        topic.ptr(),
                        partition_id as i32,
                        broker_ids.as_ptr() as *mut i32,
                        broker_ids.len(),
                        err_buf.as_mut_ptr(),
                        err_buf.len(),
                    )
                };
                check_rdkafka_invalid_arg(res, err_buf)?;
            }
        }
        for (key, val) in &self.config {
            let key_c = CString::new(*key)?;
            let val_c = CString::new(*val)?;
            let res = unsafe {
                rdsys::rd_kafka_NewTopic_set_config(topic.ptr(), key_c.as_ptr(), val_c.as_ptr())
            };
            check_rdkafka_invalid_arg(res, err_buf)?;
        }
        Ok(topic)
    }
}

/// An assignment of partitions to replicas.
///
/// Each element in the outer slice corresponds to the partition with that
/// index. The inner slice specifies the broker IDs to which replicas of that
/// partition should be assigned.
pub type PartitionAssignment<'a> = &'a [&'a [i32]];

/// Replication configuration for a new topic.
#[derive(Debug)]
pub enum TopicReplication<'a> {
    /// All partitions should use the same fixed replication factor.
    Fixed(i32),
    /// Each partition should use the replica assignment from
    /// `PartitionAssignment`.
    #[allow(dead_code)]
    Variable(PartitionAssignment<'a>),
}

#[repr(transparent)]
struct NativeNewTopic {
    ptr: *mut RDKafkaNewTopic,
}

impl NativeNewTopic {
    unsafe fn from_ptr(ptr: *mut RDKafkaNewTopic) -> NativeNewTopic {
        NativeNewTopic { ptr }
    }
}

impl WrappedCPointer for NativeNewTopic {
    type Target = RDKafkaNewTopic;

    fn ptr(&self) -> *mut RDKafkaNewTopic {
        self.ptr
    }
}

impl Drop for NativeNewTopic {
    fn drop(&mut self) {
        trace!("Destroying new topic: {:?}", self.ptr);
        unsafe {
            rdsys::rd_kafka_NewTopic_destroy(self.ptr);
        }
        trace!("New topic destroyed: {:?}", self.ptr);
    }
}

struct CreateTopicsFuture {
    rx: Oneshot<NativeEvent>,
}

impl Future for CreateTopicsFuture {
    type Item = Vec<TopicResult>;
    type Error = crate::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll() {
            Ok(Async::Ready(event)) => {
                event.check_error()?;
                let res = unsafe { rdsys::rd_kafka_event_CreateTopics_result(event.ptr()) };
                if res.is_null() {
                    let typ = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
                    return Err(crate::Error::Generic(format!(
                        "create topics request received response of incorrect type ({})",
                        typ
                    )));
                }
                let mut n = 0;
                let topics = unsafe { rdsys::rd_kafka_CreateTopics_result_topics(res, &mut n) };
                Ok(Async::Ready(build_topic_results(topics, n)))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(Canceled) => Err(crate::Error::Other(Box::new(Canceled))),
        }
    }
}

//
// Delete topic handling
//

#[repr(transparent)]
struct NativeDeleteTopic {
    ptr: *mut RDKafkaDeleteTopic,
}

impl NativeDeleteTopic {
    unsafe fn from_ptr(ptr: *mut RDKafkaDeleteTopic) -> NativeDeleteTopic {
        NativeDeleteTopic { ptr }
    }
}

impl WrappedCPointer for NativeDeleteTopic {
    type Target = RDKafkaDeleteTopic;

    fn ptr(&self) -> *mut RDKafkaDeleteTopic {
        self.ptr
    }
}

impl Drop for NativeDeleteTopic {
    fn drop(&mut self) {
        trace!("Destroying delete topic: {:?}", self.ptr);
        unsafe {
            rdsys::rd_kafka_DeleteTopic_destroy(self.ptr);
        }
        trace!("Delete topic destroyed: {:?}", self.ptr);
    }
}

struct DeleteTopicsFuture {
    rx: Oneshot<NativeEvent>,
}

impl Future for DeleteTopicsFuture {
    type Item = Vec<TopicResult>;
    type Error = crate::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.rx.poll() {
            Ok(Async::Ready(event)) => {
                event.check_error()?;
                let res = unsafe { rdsys::rd_kafka_event_DeleteTopics_result(event.ptr()) };
                if res.is_null() {
                    let typ = unsafe { rdsys::rd_kafka_event_type(event.ptr()) };
                    return Err(crate::Error::Generic(format!(
                        "delete topics request received response of incorrect type ({})",
                        typ
                    )));
                }
                let mut n = 0;
                let topics = unsafe { rdsys::rd_kafka_DeleteTopics_result_topics(res, &mut n) };
                Ok(Async::Ready(build_topic_results(topics, n)))
            }
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(Canceled) => Err(crate::Error::Other(Box::new(Canceled))),
        }
    }
}

