export function createSingleFlight(task) {
  const inFlight = new Map();
  return function run(key) {
    if (inFlight.has(key)) return inFlight.get(key);
    const promise = Promise.resolve()
      .then(() => task(key))
      .finally(() => inFlight.delete(key));
    inFlight.set(key, promise);
    return promise;
  };
}
