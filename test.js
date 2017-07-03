'use strict'

/* eslint-env mocha */

const TSDB = require('./').TSDB
const sinon = require('sinon')
const chai = require('chai')
const expect = chai.expect

describe('querying', () => {
  let tsdb
  let clock
  let now
  beforeEach(() => {
    clock = sinon.useFakeTimers(1000)
    tsdb = new TSDB()

    now = Date.now()
    for (let i = 0; i < 5; i++) {
      tsdb.series('a').insert({
        my_col1: i,
        my_col2: i * 2
      }, now - (4 - i) * 100)
    }
    for (let i = 0; i < 5; i++) {
      tsdb.series('b').insert({
        my_col1: Math.pow(i, 2)
      }, now - (4 - i) * 100)
    }
  })

  afterEach(() => clock.restore())

  it('runs map() aggregations', () => {
    expect(
      tsdb.series('a').query({
        metrics: { data: TSDB.map('my_col1') }
      })
    ).to.deep.equal([
      {
        results: {
          data: [0, 1, 2, 3, 4]
        }
      }
    ])

    expect(
      tsdb.series('a').query({
        metrics: { data: TSDB.map(pt => pt.get('my_col2')) }
      })
    ).to.deep.equal([
      {
        results: {
          data: [0, 2, 4, 6, 8]
        }
      }
    ])
  })

  it('runs mean() aggregations', () => {
    expect(
      tsdb.series('a').query({
        metrics: { mean: TSDB.mean('my_col1') }
      })
    ).to.deep.equal([
      {
        results: { mean: 2 }
      }
    ])
  })

  it('runs max() aggregations', () => {
    expect(
      tsdb.series('a').query({
        metrics: { max: TSDB.max('my_col1') }
      })
    ).to.deep.equal([
      {
        results: { max: 4 }
      }
    ])
  })

  it('runs min() aggregations', () => {
    expect(
      tsdb.series('a').query({
        metrics: { min: TSDB.min('my_col1') }
      })
    ).to.deep.equal([
      {
        results: { min: 0 }
      }
    ])
  })

  it('runs derivative() aggregations #1', () => {
    expect(
      tsdb.series('b').query({
        metrics: { der: TSDB.derivative('my_col1', 100) }
      })[0].results.der.map(pt => pt.toObject())
    ).to.deep.equal([
      { time: now - 300, my_col1: 1 },
      { time: now - 200, my_col1: 3 },
      { time: now - 100, my_col1: 5 },
      { time: now, my_col1: 7 }
    ])
  })

  it('runs derivative() aggregations #2', () => {
    expect(
      tsdb.series('b').query({
        metrics: { der: TSDB.derivative('my_col1', 200) }
      })[0].results.der.map(pt => pt.toObject())
    ).to.deep.equal([
      { time: now - 200, my_col1: 4 },
      { time: now, my_col1: 12 }
    ])
  })

  it('runs derivative() aggregations #4', () => {
    expect(
      tsdb.series('b').query({
        metrics: { der: TSDB.derivative('my_col1', 500) }
      })[0].results.der.map(pt => pt.toObject())
    ).to.deep.equal([
      { time: now, my_col1: 16 }
    ])
  })

  it('runs derivative() aggregations #5', () => {
    expect(
      tsdb.series('q').query({
        metrics: { der: TSDB.derivative('my_col1', 500) }
      })[0].results.der.map(pt => pt.toObject())
    ).to.deep.equal([])
  })

  it('runs last() aggregations', () => {
    expect(
      tsdb.series('a').query({
        metrics: { last: TSDB.last('my_col1') },
        group: TSDB.interval(250, false)
      })
    ).to.deep.equal([
      {
        results: { last: 4 },
        group: { start: now - 250, width: 250 }
      },
      {
        results: { last: 1 },
        group: { start: now - 500, width: 250 }
      }
    ])
  })

  it('runs last() aggregations', () => {
    expect(
      tsdb.series('a').query({
        metrics: { last: TSDB.last('my_col1') },
        group: TSDB.interval(250, false)
      })
    ).to.deep.equal([
      {
        results: { last: 4 },
        group: { start: now - 250, width: 250 }
      },
      {
        results: { last: 1 },
        group: { start: now - 500, width: 250 }
      }
    ])
  })

  it('time shifts the interval grouper', () => {
    expect(
      tsdb.series('a').query({
        metrics: { data: TSDB.map('my_col1') },
        group: TSDB.interval(250, false, now - 100)
      })
    ).to.deep.equal([
      {
        results: { data: [1, 2, 3] },
        group: { start: now - 350, width: 250 }
      },
      {
        results: { data: [0] },
        group: { start: now - 600, width: 250 }
      }
    ])
  })

  it('runs grouped() aggregations #1', () => {
    expect(
      tsdb.series('a').query({
        metrics: { data: TSDB.map('my_col1') },
        group: TSDB.interval(250, false)
      })
    ).to.deep.equal([
      {
        results: { data: [2, 3, 4] },
        group: { start: now - 250, width: 250 }
      },
      {
        results: { data: [0, 1] },
        group: { start: now - 500, width: 250 }
      }
    ])
  })

  it('runs grouped() aggregations #2', () => {
    expect(
      tsdb.series('a').query({
        metrics: { data: TSDB.map('my_col1') },
        group: TSDB.interval(100, false),
        where: { time: { is: '>', than: now - 300 } }
      })
    ).to.deep.equal([
      {
        results: { data: [4] },
        group: { start: now - 100, width: 100 }
      },
      {
        results: { data: [3] },
        group: { start: now - 200, width: 100 }
      },
      {
        results: { data: [2] },
        group: { start: now - 300, width: 100 }
      }
    ])
  })

  it('runs grouped() aggregations #3', () => {
    expect(
      tsdb.series('a').query({
        metrics: { data: TSDB.map('my_col1') },
        group: TSDB.interval(100, false),
        where: { time: { is: '>', than: now } }
      })
    ).to.deep.equal([])
  })

  it('respects single where clauses', () => {
    expect(
      tsdb.series('a').query({
        metrics: { data: TSDB.map('my_col1') },
        where: { time: { is: '>', than: now - 300 } }
      })
    ).to.deep.equal([
      {
        results: {
          data: [2, 3, 4]
        }
      }
    ])
  })

  it('respects multiple where clauses', () => {
    expect(
      tsdb.series('a').query({
        metrics: { data: TSDB.map('my_col1') },
        where: {
          time: [
            { is: '>', than: now - 300 },
            { is: '<', than: now }
          ]
        }
      })
    ).to.deep.equal([
      {
        results: {
          data: [2, 3]
        }
      }
    ])
  })

  it('drops all data', () => {
    tsdb.series('a').remove()
    expect(
      tsdb.series('a').query({
        metrics: { total: TSDB.count() }
      })
    ).to.deep.equal([
      {
        results: {
          total: 0
        }
      }
    ])
  })

  it('drops data matching a clause', () => {
    tsdb.series('a').remove({
      time: [
        { is: '>', than: now - 300 },
        { is: '<', than: now }
      ]
    })

    expect(
      tsdb.series('a').query({
        metrics: { time: TSDB.map('time') }
      })
    ).to.deep.equal([
      {
        results: {
          time: [600, 700, 1000]
        }
      }
    ])
  })
})
