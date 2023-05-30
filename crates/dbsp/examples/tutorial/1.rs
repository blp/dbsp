use dbsp::RootCircuit;

fn main() {
    let (_circuit, ()) = RootCircuit::build(|_circuit| Ok(())).unwrap();
}
