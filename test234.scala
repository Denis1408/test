if (fail > 0) {
  throw new RuntimeException(
    s"$fail notebook executions failed; $ok succeeded. Check notebook output for details."
  )
}
