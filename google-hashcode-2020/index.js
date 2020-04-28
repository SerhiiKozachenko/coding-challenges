const fs = require('fs').promises
const util = require('util')

const prn = (obj) => console.log(util.inspect(obj, false, null, true /* enable colors */))

const scoreLib = (scores, libBookIds) => {
  // console.log('scores', scores)
  // console.log('libBookIds', libBookIds)
  const totalScore = libBookIds.reduce((res, i) =>{
    return res + scores[i]
  }, 0)

  // console.log('totalScore', totalScore)
  return totalScore
}


const parse = input => {
  const lines = input.split('\n')
  const totals = lines[0].split(' ').map(x=>Number(x))
  const scores = lines[1].split(' ').map(x=>Number(x))

  const totalDays = totals[2]
  const libs = lines.slice(2, lines.length - 1)

  // console.log('totals', totals)
  // console.log('totals[0]', totals[0])
  // console.log('scores', scores)
  // console.log('score of book with id 3 = ', scores[3])

  let parsedLibs = []
  let libId = 0;
  let readBooks = new Set()
  let totalDaysToSignup = 0
  for (let i = 0; i< libs.length; i = i + 2) {
    const libMetaLine = libs[i].split(' ').map(x=>Number(x))
    const libBooksLine = libs[i+1]
    if (!libBooksLine) {
      continue
    }

    const libBookIds = libBooksLine.split(' ').map(x=>Number(x))

    const days_to_signup = Number(libMetaLine[1])

    const scan_books_per_day = Number(libMetaLine[2])

    totalDaysToSignup = totalDaysToSignup + days_to_signup
    const total_books_can_be_scanned = scan_books_per_day * (totalDays - totalDaysToSignup)

    const unreadBookIds = libBookIds.reduce((res, i) => {
      if (!readBooks.has(i) && total_books_can_be_scanned > res.length) {
        readBooks.add(i)
        res.push(i)
      }
      return res
    }, [])

    if (unreadBookIds.length) {
      parsedLibs.push({
        meta: {
          id: libId,
          books_qty: unreadBookIds.length,
          days_to_signup,
          scan_books_per_day,
          total_score: scoreLib(scores, unreadBookIds),
          total_books_can_be_scanned
        },
        // sort book ids so that with higher scores go first
        books: unreadBookIds.sort((a, b) => {
          // desc
          return scores[b] - scores[a]
        })
      })
    }

    libId++;
  }

  return {
    total: {
      books: totals[0],
      libs: totals[1],
      days: totalDays
    },
    scores,
    libs: parsedLibs
  }
}


const result = async (fname, libs) => {
  const fileName = `${fname}_output.txt`
  await fs.writeFile(fileName, `${libs.length}\n`)
  for (let i = 0; i< libs.length; i++) {
    await fs.appendFile(fileName, `${libs[i].meta.id} ${libs[i].meta.books_qty}\n`)
    await fs.appendFile(fileName, `${libs[i].books.join(' ')}\n`)
  }
}

const main = async (fileName) => {
  const input = await fs.readFile(`${fileName}.txt`, 'ascii')

  // prn(input)

  const parsed = parse(input)
  prn(parsed.total)

  // TODO: process

   // -1 = [a, b]

    // 1 = [b, a]

    // 0 = no op
    // desc
    // return b - a

    // asc
    // eturn a - b

    
  const sortedLibs = parsed.libs
  .sort((a, b) =>
  // more total_score first
  b.meta.total_books_can_be_scanned - a.meta.total_books_can_be_scanned
)
    //   .sort((a, b) =>
    //     // less days to signup first
    //     a.meta.days_to_signup - b.meta.days_to_signup
    // )
//   .sort((a, b) =>
//   // more scan_books_per_day first
//   b.meta.scan_books_per_day - a.meta.scan_books_per_day
// )
  .sort((a, b) =>
      // more total_score first
      b.meta.total_score - a.meta.total_score
    )
  // .sort((a, b) =>
  //     // more books first
  //     b.meta.books_qty - a.meta.books_qty
  //   )
    // .sort((a, b) =>
    //     // less days to signup first
    //     a.meta.days_to_signup - b.meta.days_to_signup
    // )

    // more scan_books_per_day first

    


  console.log(sortedLibs[0].meta.total_score)
  console.log(sortedLibs[1].meta.total_score)
  // console.log(sortedLibs[2].meta.days_to_signup)
  // console.log(sortedLibs[20].meta.total_score)
  console.log(sortedLibs[sortedLibs.length-2].meta.total_score)
  console.log(sortedLibs[sortedLibs.length-1].meta.total_score)

  await result(fileName, sortedLibs)
}


// main('a_example').catch(console.error)
main('b_read_on').catch(console.error)
main('c_incunabula').catch(console.error)
main('d_tough_choices').catch(console.error)
main('e_so_many_books').catch(console.error)
main('f_libraries_of_the_world').catch(console.error)
