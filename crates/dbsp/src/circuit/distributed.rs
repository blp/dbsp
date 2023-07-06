mod input {
    /// An input sequence number in a DBSP circuit, starting with 0 and
    /// incrementing with each item.  Each input session has a separate
    /// namespace for sequence numbers.
    pub type Sequence = u64;

    /// An identifier for a DBSP input session.  Generated randomly by the
    /// client.
    pub type Session = uuid;

    pub type InputHandle = u64;

    pub trait Producer {
        /// Creates or renews the given input `session`.  The `comment` should
        /// be a hint to aid a human to understand why the input session
        /// was created.  On success, returns the next sequence number
        /// that may be submitted as input for this session (if this is
        /// a new session, this will be 0).  On failure, returns an
        /// error (particular error type TBD).
        async fn new_session(
            &mut self,
            session: Session,
            comment: &str,
        ) -> Result<Sequence, ()>;

        /// Explicitly deletes the given input `session`.  Input sessions expire
        /// automatically after a time (e.g. 30 days) if not otherwise used.
        ///
        /// Input sessions are lightweight, so it is not essential to delete
        /// stray sessions.
        async fn drop_session(&mut self, session: Session) -> Result<(), ()>;

        /// For Z-set and indexed Z-set input handles only, appends the
        /// key-value pairs in `values` to `input_handle` within the circuit.
        /// Returns `Ok(())` if `values` has been committed to stable storage
        /// such that its output is guaranteed to become available in a future
        /// output, or `Err(expected_seq)`, if `seq` was wrong and
        /// `expected_seq` was its expected value.
        ///
        /// # Concurrency
        ///
        /// This method divides `values` into a group for each worker, and then
        /// pushes each group to its worker.  Each group is atomically appended
        /// at each worker, but concurrent `append`s A and B may apply in
        /// different orders at different workers and concurrent `append` and
        /// `run` may result in data from `append` being input in different
        /// steps.  When the latter happens, `append` returns an output sequence
        /// number when all of `values` is guaranteed to have been processed (in
        /// some cases, this might be greater than the minimal sequence number).
        async fn append<K, V>(
            &mut self,
            seq: Sequence,
            input_handle: InputHandle,
            values: Vec<(K, V)>,
        ) -> Result<(), Sequence>;

        async fn set<T>(
            &mut self,
            seq: Sequence,
            input_handle: InputHandle,
            value: T,
        ) -> Result<(), Sequence>;

        /// Ensures that input submitted through this handle up through and
        /// including `seq` has been started running through the circuit.
        /// Returns `Ok(output_seq)` if the output for `seq` (or later) will be
        /// available as `output_seq` (or earlier), or `Err(max_seq)` if `seq`
        /// is too big and `max_seq` is the greatest value that could be
        /// accepted.
        async fn run(
            &mut self,
            seq: Sequence,
        ) -> Result<crate::output::Sequence, Sequence>;

        /// Feeds `input` into the circuit for input sequence number `seq` in
        /// `session`.  Returns `Ok(output_seq)` only when `input` has been
        /// committed to stable storage such that its output is guaranteed to
        /// become available as `output_seq`, or `Err(expected_seq)`, if `seq`
        /// was wrong and `expected_seq` was its expected value.
        async fn input(
            &mut self,
            session: Session,
            seq: Sequence,
            input: I,
        ) -> Result<crate::output::Sequence, Sequence>;
    }
}

mod output {
    /// An output sequence number in a DBSP circuit, starting with 0 and
    /// incrementing with each output item.
    pub type Sequence = u64;

    /// An identifier for a DBSP output session.  Generated randomly by the
    /// client.
    pub type Session = uuid;

    pub trait Consumer<O> {
        /// Creates or renews the given output `session`.  The `comment` should
        /// be a hint to aid a human to understand why the output
        /// session was created. On success, returns the range of output
        /// sequence numbers that may be requested in this session (for
        /// a new session, this is an empty range).  On failure, returns
        /// an error (particular error type TBD).
        async fn new_session(
            &mut self,
            session: Session,
            comment: &str,
        ) -> Result<Range<Sequence>, ()>;

        /// Explicitly deletes the given output `session`.  Output sessions
        /// expire automatically after a time (e.g. 30 days) if not otherwise
        /// used.
        ///
        /// It is important to delete output sessions that are no longer used,
        /// because they prevent output from being discarded.
        async fn drop_session(&mut self, session: Session) -> Result<(), ()>;

        /// Obtains the output from the circuit for sequence number `seq`.
        /// Returns `Ok(output)` when the output for `seq` becomes
        /// available and stable.  If we haven't reached sequence number
        /// `seq` yet, or if `session` has acknowledged `seq`, returns
        /// `Err(range)` where `range` is the range of sequence numbers
        /// for which output is currently available within `session`.
        async fn output(&mut self, session: Session, seq: Sequence) -> Result<O, Range<Sequence>>;

        /// Acknowledges all of the output from the circuit up to and including
        /// step `seq` within `session`.
        async fn ack(&mut self, session: Session, seq: Sequence) -> Result<(), ()>;
    }
}
